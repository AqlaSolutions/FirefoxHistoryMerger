using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FirefoxHistoryMerger
{
    class Program
    {
        static void Main(string[] args)
        {
            var f = new FirefoxHistoryMerger { ApplyChanges = true };
            string currentName = "current.sqlite";
            f.CombineHistory(
                Directory.GetFiles(@"D:\places\", "*.sqlite")
                    .Where(fn => !string.Equals(Path.GetFileName(fn), currentName, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(fn => fn)
                    .ToArray(),
                @"D:\places\" + currentName,
                args.Length == 1 ? args[0] : string.Empty);
            Console.WriteLine();
            Console.WriteLine("Completed.");
            Console.ReadLine();
        }
    }
}