using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Util;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DynamoDBTest.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
          "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly IDynamoDBContext _dynamoDBContext;
        private readonly IAmazonDynamoDB _dynamoDbClient;
        public WeatherForecastController(ILogger<WeatherForecastController> logger, IDynamoDBContext dynamoDBContext
            ,IAmazonDynamoDB dynamoDBClient
            )
        {
            _logger = logger;
            _dynamoDBContext = dynamoDBContext;
            _dynamoDbClient = dynamoDBClient;
        }

        #region DynamoDB Create Read Update Delete

        #region Get Selected Date Weather Record
        [HttpGet(Name = "GetWeatherForecast")]
        public async Task<IEnumerable<WeatherForecast>> Get(string City = "Peshawar")
        {
             return await _dynamoDBContext.QueryAsync<WeatherForecast>(City,QueryOperator.BeginsWith, new object[] { "2023-08-17"})
                .GetRemainingAsync();

            //return GenerateDummyWeatherForeCast(City);
        }
        #endregion

        #region Read All Data from Table

        /*public async Task<IEnumerable<WeatherForecast>> Get()
          {
              return await _dynamoDBContext.QueryAsync<WeatherForecast>("")
                 .GetRemainingAsync();

              //return GenerateDummyWeatherForeCast(City);
          }*/
        #endregion

        #region Save Weather Forecast in DynamoDB Table
        //[HttpPost(Name = "GetWeatherForecast")]
        //public async Task Post(string city)
        //{
        //    var data = GenerateDummyWeatherForeCast(city);
        //    foreach (var item in data)
        //    {
        //        await _dynamoDBContext.SaveAsync(item);
        //    }
        //}
        #endregion
        
        #region Save Weather Forecast in DynamoDB Table
        [HttpPost(Name = "GetWeatherForecast")]
        public async Task Post(WeatherForecast weatherForecast)
        {
          
                await _dynamoDBContext.SaveAsync(weatherForecast);
            
        }
        #endregion

        #region Update Single Weather Forecast in DynamoDB Table
        [HttpPut(Name = "GetWeatherForecast")]
        public async Task Put(string city)
        {   
            var specified=   await _dynamoDBContext.LoadAsync<WeatherForecast>(city,"2023-08-17");
            specified.Summary = "Test";
            await _dynamoDBContext.SaveAsync(specified);
        }
        #endregion

        #region Delete Single Weather Forecast in DynamoDB Table
        [HttpDelete(Name = "GetWeatherForecast")]
        public async Task Delete(string city)
        {
            var specified = await _dynamoDBContext.LoadAsync<WeatherForecast>(city, "2023-08-18");
            await _dynamoDBContext.DeleteAsync(specified);
        }
        #endregion

        #region Generate Weather ForeCast Dummy Data
        private static IEnumerable<WeatherForecast> GenerateDummyWeatherForeCast(string City)
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                City = City,
                Date = DateTime.Now.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }
        #endregion

        #endregion

        #region  DynamoDB Pagination 

        [HttpGet("city-name-paged")]
        public async Task<PagedResult<WeatherForecast>> GetAllForCityPaged(string city, string? pageKey)
        {
            var exlusiveStartKey = string.IsNullOrEmpty(pageKey);

            var queryRequest = new QueryRequest()
            {
                TableName = nameof(WeatherForecast),
                KeyConditionExpression = "City = :city",
                Limit = 5,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":city",new AttributeValue(city)},
                }
            };

            var response = await _dynamoDbClient.QueryAsync(queryRequest);
            var items = response.Items.Select(a => {
                var doc = Document.FromAttributeMap(a);
                return _dynamoDBContext.FromDocument<WeatherForecast>(doc);
            }).ToList();

            var nextPageKey = response.LastEvaluatedKey.Count ==0
                ? null
                : Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(
                    response.LastEvaluatedKey,
                    new JsonSerializerOptions()
                    {
                        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault
                    }
                    ));
            return new PagedResult<WeatherForecast>()
            {
                Items = items,
                NextPageKey = nextPageKey
            };
        }

        [HttpGet("city-name-paged-range-key")]
        public async Task<PagedResult<WeatherForecast>> GetAllForCityPagedWithRangeKey(string city,
            DateTime? fromDateTime, bool isForward = true)
        {
            Dictionary<string, AttributeValue> exlusiveStartKey = null;
            if (fromDateTime.HasValue)
            {
                var doc = new Document();
                doc[nameof(WeatherForecast.City)] = city;
                doc[nameof(WeatherForecast.Date)] = fromDateTime.Value.ToString(AWSSDKUtils.ISO8601DateFormat);
                exlusiveStartKey = doc.ToAttributeMap();
            }

            var queryRequest = new QueryRequest()
            {
                TableName = nameof(WeatherForecast),
                KeyConditionExpression = "City = :city",
                Limit = 5,
                ScanIndexForward = isForward,
                ExclusiveStartKey = exlusiveStartKey,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":city", new AttributeValue(city)},
                }
            };
            var response = await _dynamoDbClient.QueryAsync(queryRequest);
            var items = response.Items.Select(a => {
                var doc = Document.FromAttributeMap(a);
                return _dynamoDBContext.FromDocument<WeatherForecast>(doc);
            }).ToList();

            var nextPageKey = !response.LastEvaluatedKey.Any()
                ? null
                : isForward 
                ? items.MaxBy(a => a.Date)?.Date.ToString(AWSSDKUtils.ISO8601DateFormat)
                : items.MinBy(a => a.Date)?.Date.ToString(AWSSDKUtils.ISO8601DateFormat);

            return new PagedResult<WeatherForecast>()
            {
                Items = items,
                NextPageKey = nextPageKey
            };
        }

        [HttpGet("city-name-paged-key-condition")]
        public async Task<PagedResult<WeatherForecast>> GetAllForCityPagedWithKeycondition(string city,
            DateTime? fromDateTime)
        {
            Dictionary<string, AttributeValue> exlusiveStartKey = null;
            if (fromDateTime.HasValue)
            {
                var doc = new Document();
                doc[nameof(WeatherForecast.City)] = city;
                doc[nameof(WeatherForecast.Date)] = fromDateTime.Value.ToString(AWSSDKUtils.ISO8601DateFormat);
                exlusiveStartKey = doc.ToAttributeMap();
            }

            var queryRequest = new QueryRequest()
            {
                TableName = nameof(WeatherForecast),
                KeyConditionExpression = "City = :city and #Date > :fromDateTime",
                Limit = 5,
                ExpressionAttributeNames = new Dictionary<string,string>()
                {
                    { "#Date","Date" }
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                {
                    {":city", new AttributeValue(city)},
                    { ":fromDateTime" , new AttributeValue(fromDateTime.Value.ToString(AWSSDKUtils.ISO8601DateFormat)) },
                }
            };
            var response = await _dynamoDbClient.QueryAsync(queryRequest);
            var items = response.Items.Select(a => {
                var doc = Document.FromAttributeMap(a);
                return _dynamoDBContext.FromDocument<WeatherForecast>(doc);
            }).ToList();

            var nextPageKey = !response.LastEvaluatedKey.Any()
                ? null
                : items.MaxBy(a => a.Date)?.Date.ToString(AWSSDKUtils.ISO8601DateFormat);

            return new PagedResult<WeatherForecast>()
            {
                Items = items,
                NextPageKey = nextPageKey
            };
        }
        #endregion
    
    
    }
}