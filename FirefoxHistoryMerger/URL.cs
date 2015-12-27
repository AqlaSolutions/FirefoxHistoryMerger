namespace FirefoxHistoryMerger
{
    public class URL
    {
        string url;
        string title;
        string browser;
        public URL(string url, string title, string browser)
        {
            this.url = url;
            this.title = title;
            this.browser = browser;
        }
    }
}