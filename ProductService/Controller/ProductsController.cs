using Elastic.Clients.Elasticsearch.Nodes;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using Newtonsoft.Json;
using ProductService.Data;
using StackExchange.Redis;
using System.Net;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;

[ApiController]
[Route("api/[controller]")]
public class ProductsController : ControllerBase
{
    private readonly AppDbContext _context;
    private readonly IDatabase _redis; 
    private readonly ElasticsearchClient _esClient;

    public ProductsController(AppDbContext context, ElasticsearchClient esClient)
    {
        _context = context;
        _esClient = esClient;

        var mux = ConnectionMultiplexer.Connect("localhost:6379");
        _redis = mux.GetDatabase();
    }

    [HttpGet("GetAll")]
    public async Task<IActionResult> GetAll()
    {
        var redisProducts = await _redis.HashGetAllAsync("products_list");

        if (redisProducts.Length > 0)
        {
            var products = redisProducts
                .Select(p => JsonConvert.DeserializeObject<Product>(p.Value))
                .ToList();
            return Ok(products);
        }

        var dbProducts = await _context.Products.ToListAsync();

        foreach (var product in dbProducts)
        {
            await _redis.HashSetAsync(
                "products_list",
                product.Id,
                JsonConvert.SerializeObject(product)
            );
        }

        return Ok(dbProducts);
    }

    [HttpGet("GetById/{id}")]
    public async Task<IActionResult> GetById(int id)
    {
        var cachedProduct = await _redis.HashGetAsync("products_list", id);

        Product? product;
        if (!cachedProduct.IsNull)
        {
            product = JsonConvert.DeserializeObject<Product>(cachedProduct);
        }
        else
        {
            product = await _context.Products.FindAsync(id);
            if (product == null) return NotFound();

            await _redis.HashSetAsync("products_list", id, JsonConvert.SerializeObject(product));
        }

        await _redis.SortedSetIncrementAsync("popular_products", id.ToString(), 1);

        return Ok(product);
    }

    [HttpPost("Create")]
    public async Task<IActionResult> Create(Product product)
    {
        _context.Products.Add(product);
        await _context.SaveChangesAsync();

        await _redis.HashSetAsync("products_list", product.Id, JsonConvert.SerializeObject(product));

        var response = await _esClient.IndexAsync(product, i => i.Index("products"));

        if (!response.IsValidResponse)
            return BadRequest(response.DebugInformation);

        return Ok(product);
    }

    [HttpPut("Update")]
    public async Task<IActionResult> Update( Product updated)
    {
        var product = await _context.Products.FindAsync(updated.Id);
        if (product == null) return NotFound();
        if (!string.IsNullOrEmpty(updated.Name))
        {
            product.Name = updated.Name;
        }
        if (!string.IsNullOrEmpty(updated.Description))
        {
            product.Description = updated.Description;

        }
        if (updated.Stock!=0)
        {
            product.Stock = updated.Stock;
        }
        if (updated.Price != 0)
        {
            product.Price = updated.Price;

        }
        product.UpdatedAt = DateTime.UtcNow;

        await _context.SaveChangesAsync();

        await _redis.HashSetAsync("products_list", product.Id, JsonConvert.SerializeObject(product));

        return Ok(product);
    }

    [HttpDelete("Delete/{id}")]
    public async Task<IActionResult> Delete(int id)
    {
        var product = await _context.Products.FindAsync(id);
        if (product == null) return NotFound();

        _context.Products.Remove(product);
        await _context.SaveChangesAsync();

        await _redis.HashDeleteAsync("products_list", id);

        return NoContent();
    }

    [HttpGet("popular")]
    public async Task<IActionResult> GetPopular([FromQuery] int count = 5)
    {
        var topIds = await _redis.SortedSetRangeByRankAsync(
            "popular_products",
            order: Order.Descending,
            start: 0,
            stop: count - 1
        );

        var products = new List<Product>();

        foreach (var id in topIds)
        {
            var cachedProduct = await _redis.HashGetAsync("products_list", id);
            if (!cachedProduct.IsNull)
            {
                var product = JsonConvert.DeserializeObject<Product>(cachedProduct);
                if (product != null) products.Add(product);
            }
        }

        return Ok(products);
    }

    [HttpGet("search")]
    public async Task<IActionResult> Search(string query, [FromServices] ElasticsearchClient esClient)
    {
        var response = await esClient.SearchAsync<Product>(s => s
            .Index("products")
            .Query(q => q
                .MultiMatch(m => m
                    .Fields(new[] { "name", "category", "description" })
                    .Query(query)
                )
            )
        );

        if (!response.IsValidResponse)
            return BadRequest(response.DebugInformation);

        return Ok(response.Documents);
    }

}
