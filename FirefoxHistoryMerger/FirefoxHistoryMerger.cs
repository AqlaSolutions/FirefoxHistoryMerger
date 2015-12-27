using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;

namespace FirefoxHistoryMerger
{
    using Entity = Dictionary<string, object>;

    public class FirefoxHistoryMerger
    {
        public bool ApplyChanges { get; set; }

        void Insert(SQLiteConnection connection, string table, Entity entity, ref SQLiteCommand cmd)
        {
            if (entity.Count == 0) return;
            if (cmd == null)
            {
                var columns = entity.Keys.ToArray();
                cmd = new SQLiteCommand(
                    string.Format("INSERT INTO {0} ({1}) VALUES ({2})", table, string.Join(",", columns), string.Join(",", columns.Select(c => "@" + c))),
                    connection);
            }
            else cmd.Parameters.Clear();
            foreach (var v in entity)
                cmd.Parameters.AddWithValue("@" + v.Key, v.Key == "id" ? DBNull.Value : v.Value);
            cmd.ExecuteNonQuery();
        }

        public void CombineHistory(string[] placesFiles, string appendToFile, string whereSql = "")
        {
            const string mozPlaces = "moz_places";
            const string mozHistoryvisits = "moz_historyvisits";

            var finalPlaces = ReadTable("SELECT * FROM " + mozPlaces, appendToFile);
            var finalPlacesByUrl = finalPlaces.Values.ToDictionary(v => (string)v["url"]);
            var finalVisits = ReadTable("SELECT * FROM " + mozHistoryvisits, appendToFile);

            var finalVisitsByPlaces = finalVisits.GroupBy(v => Convert.ToInt32(v.Value["place_id"])).ToDictionary(g => g.Key, g => g.ToDictionary(kv => kv.Key, kv => kv.Value));
            Console.WriteLine("Opening main file " + appendToFile);
            SelfCheck("(start)", finalPlaces, finalVisits);
            Console.WriteLine();
            DoWithDb(
                appendToFile,
                connection =>
                    {
                        foreach (var placesFile in placesFiles)
                        {
                            var insertPlaces = ReadTable("SELECT * FROM " + mozPlaces + whereSql, placesFile);
                            var insertVisits = ReadTable("SELECT * FROM " + mozHistoryvisits, placesFile)
                                .Where(v => insertPlaces.ContainsKey(Convert.ToInt32(v.Value["place_id"])))
                                .ToDictionary(kv => kv.Key, kv => kv.Value);

                            Console.WriteLine("Opening " + placesFile);
                            SelfCheck("(input)", insertPlaces, insertVisits);
                            var placeRedirects = new Dictionary<int, int>();

                            // удаляем уже существующие записи в places
                            foreach (var placeKV in insertPlaces.ToArray())
                            {
                                Entity placeOld;
                                // оставляем если нет такого place под этим id
                                if (!finalPlaces.TryGetValue(placeKV.Key, out placeOld) || !placeKV.Value["url"].Equals(placeOld["url"]))
                                {
                                    // и нет под другим id
                                    if (!finalPlacesByUrl.TryGetValue((string)placeKV.Value["url"], out placeOld))
                                        continue;
                                    placeRedirects.Add(placeKV.Key, Convert.ToInt32(placeOld["id"]));
                                }
                                insertPlaces.Remove(placeKV.Key);
                            }

                            // удаляем уже существующие записи в visits
                            foreach (var visitKV in insertVisits.ToArray())
                            {
                                var visit = visitKV.Value;

                                var placeId = Convert.ToInt32(visit["place_id"]);
                                int newPlaceId;

                                if (placeRedirects.TryGetValue(placeId, out newPlaceId))
                                    placeId = newPlaceId;

                                Dictionary<int, Entity> visits;
                                if (!finalVisitsByPlaces.TryGetValue(placeId, out visits)) continue;

                                // если нет похожих, то только тогда оставляем
                                if (visits.Values.All(v => !v["visit_date"].Equals(visit["visit_date"]) || !v["visit_type"].Equals(visit["visit_type"]))) continue;

                                //Entity visitOld;
                                //// оставляем если такого visit под этим id нет
                                //// да, при добавлении будут создаваться новые id
                                //// т.е. но мы хотя бы избегаем копирования записей из "историч. исходников"
                                //if (!finalVisits.TryGetValue(Convert.ToInt32(visit["id"]), out visitOld)) continue;
                                //if (!AreEntitiesEqual(visit, visitOld)) continue;

                                //// visit равны, но может быть не равны places?
                                //var placeId = Convert.ToInt32(visit["place_id"]);
                                //int newPlaceId;

                                //// если место есть под другим id, то оставляем визит, т.е. места-то не совпадают под старым id
                                //if (placeRedirects.TryGetValue(placeId, out newPlaceId)) continue;

                                //// оставляем если это новое место
                                //Entity placeOld;
                                //if (!finalPlaces.TryGetValue(placeId, out placeOld)) continue;

                                //Entity place;
                                //// оставляем если в final нет такого же place под тем же id (т.е. он остался в списке insert)
                                //if (insertPlaces.TryGetValue(placeId, out place)) continue;

                                insertVisits.Remove(visitKV.Key);
                            }
                            Console.WriteLine("Writing " + insertPlaces.Count + " places and " + insertVisits.Count + " visits from " + placesFile);
                            SQLiteCommand cmd = null;
                            foreach (var placeKV in insertPlaces)
                            {
                                Insert(connection, mozPlaces, placeKV.Value, ref cmd);
                                int id = (int)connection.LastInsertRowId;
                                placeKV.Value["id"] = id;
                                placeRedirects.Add(placeKV.Key, id);
                                finalPlaces.Add(id, placeKV.Value);
                                finalPlacesByUrl.Add((string)placeKV.Value["url"], placeKV.Value);
                            }
                            cmd = null;
                            foreach (var visitKV in insertVisits)
                            {
                                var placeId = Convert.ToInt32(visitKV.Value["place_id"]);
                                int newPlaceId;
                                if (placeRedirects.TryGetValue(placeId, out newPlaceId))
                                    visitKV.Value["place_id"] = placeId = newPlaceId;
                                Insert(connection, mozHistoryvisits, visitKV.Value, ref cmd);
                                int id = (int)connection.LastInsertRowId;
                                visitKV.Value["id"] = id;
                                finalVisits.Add(id, visitKV.Value);
                                Dictionary<int, Entity> visits;
                                if (!finalVisitsByPlaces.TryGetValue(placeId, out visits))
                                    finalVisitsByPlaces.Add(placeId, visits = new Dictionary<int, Entity>());
                                visits.Add(id, visitKV.Value);
                            }

                            SelfCheck("(result)", finalPlaces, finalVisits);
                            Console.WriteLine();
                        }
                    });
        }

        static void SelfCheck(string name, Dictionary<int, Entity> places, Dictionary<int, Entity> visits)
        {
            var visitsByPlaceId = visits.GroupBy(v => Convert.ToInt32(v.Value["place_id"])).ToDictionary(g => g.Key, g => g.ToArray());
            var placesWithoutVisits = places.Where(p => !visitsByPlaceId.ContainsKey(p.Key)).ToArray();
            var visitsWithMissedPlaces = visits.Where(v => !places.ContainsKey(Convert.ToInt32(v.Value["place_id"]))).ToArray();
            Console.WriteLine("Self check {2}: {0} places without visits and {1} visits with missed places", placesWithoutVisits.Length, visitsWithMissedPlaces.Length, name);
        }
        
        Dictionary<int, Entity> ReadTable(string query, string file)
        {
            var dt = DoWithDb(
                file,
                c =>
                    {
                        var tableLocal = new DataTable();
                        // Populate Data Table
                        var db = new SQLiteDataAdapter(query, c);
                        db.Fill(tableLocal);
                        return tableLocal;
                    });


            var columns = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
            return dt.AsEnumerable()
                .Select(row => row.ItemArray.Select((v, i) => new { Column = columns[i], Value = v }).ToDictionary(v => v.Column, v => v.Value))
                .ToDictionary(v => Convert.ToInt32(v["id"]));
        }

        void DoWithDb(string file, Action<SQLiteConnection> action)
        {
            DoWithDb(
                file,
                c =>
                    {
                        using (var tr = c.BeginTransaction())
                        {
                            try
                            {
                                action(c);
                            }
                            catch
                            {
                                tr.Rollback();
                                throw;
                            }
                            if (ApplyChanges)
                                tr.Commit();
                            else
                                tr.Rollback();
                        }
                        return true;
                    });
        }

        T DoWithDb<T>(string file, Func<SQLiteConnection, T> action)
        {
            SQLiteConnection sqlCon;

            using (sqlCon = new SQLiteConnection(string.Format("Data Source={0};Version=3;New={1};Compress=True;", file, !File.Exists(file) ? "True" : "False")))
            {
                sqlCon.Open();
                return action(sqlCon);
            }
        }
    }
}