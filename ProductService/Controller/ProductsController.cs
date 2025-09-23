using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Distributed;
using Newtonsoft.Json;
using ProductService.Data;

[ApiController]
[Route("api/[controller]")]
public class ProductsController : ControllerBase
{
    private readonly AppDbContext _context;
    private readonly IDistributedCache _cache;

    public ProductsController(AppDbContext context, IDistributedCache cache)
    {
        _context = context;
        _cache = cache;
    }

    [HttpGet]
    public async Task<IActionResult> GetAll()
    {
        var cacheKey = "products_list";
        var cached = await _cache.GetStringAsync(cacheKey);

        if (!string.IsNullOrEmpty(cached))
            return Ok(JsonConvert.DeserializeObject<List<Product>>(cached));

        var products = _context.Products.ToList();
        await _cache.SetStringAsync("products_list", JsonConvert.SerializeObject(products));

        return Ok(products);
    }

    [HttpPost]
    public IActionResult Create(Product product)
    {
        _context.Products.Add(product);
        _context.SaveChanges();

        _cache.Remove("products_list");

        return Ok(product);
    }
}
