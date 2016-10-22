# FirefoxHistoryMerger
Allows to merge Firefox history data from multiple files (you can merge backups)

You can also set places.history.expiration.transient_current_max_pages and places.history.expiration.max_pages to big number in about:config to prevent Firefox from deleting your history.

It can be compiled with Visual Studio 2015 Community. There is no UI, parameters are hardcoded in Program.cs. I haven't put my effort to make it into end-user application.

It looks in "D:\places\" for sqlite files (ordered by name) and appends all their content to "D:\places\current.sqlite". Put there different versions of your places.sqlite files from your firefox profile folder. The latest version should be "current.sqlite".

You can also improve the code and submit a pull request.
