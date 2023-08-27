namespace DynamoDBTest
{
    public class PagedResult<T> where T : class
    {
        public List<T> Items { get; set; }
        public string NextPageKey { get; set; }
    }
}
