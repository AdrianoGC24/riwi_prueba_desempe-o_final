import os
import json
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(
    title="DataMart Legacy Catalog API",
    description="API REST interna del sistema legado de DataMart para consulta de catálogo de productos.",
    version="1.0.0"
)

# Modelo del producto 
class Product(BaseModel):
    stock_code: str
    name: str
    category: str
    supplier_country: str
    is_active: bool

# Carga en memoria del catálogo desde el seed JSON
CATALOG_DB = {}
SEED_FILE = os.path.join(os.path.dirname(__file__), "products_seed.json")

if os.path.exists(SEED_FILE):
    with open(SEED_FILE, "r", encoding="utf-8") as f:
        items = json.load(f)
        for item in items:
            CATALOG_DB[item["stock_code"].upper().strip()] = item


@app.get("/health", tags=["Health"])
def health_check():
    """Endpoint para verificar el estado de salud del servicio en Docker."""
    return {"status": "healthy", "total_products": len(CATALOG_DB)}


@app.get("/products", tags=["Products"])
def get_products(
    page: int = Query(1, ge=1, description="Número de página"),
    limit: int = Query(50, ge=1, le=10000, description="Cantidad de productos por página")
):
    """
    Endpoint 1 (Requisito PDF): Devuelve el listado de productos con paginación.
    """
    all_products = list(CATALOG_DB.values())
    total = len(all_products)
    start = (page - 1) * limit
    end = start + limit
    paginated_items = all_products[start:end]

    return {
        "page": page,
        "limit": limit,
        "total": total,
        "items": paginated_items
    }


@app.get("/products/{stock_code}", response_model=Product, tags=["Products"])
def get_product_by_code(stock_code: str):
    """
    Endpoint 2 (Requisito PDF): Permite consultar un producto por su código normalizado.
    """
    code_normalized = stock_code.upper().strip()
    if code_normalized not in CATALOG_DB:
        raise HTTPException(status_code=404, detail=f"Producto con código '{stock_code}' no encontrado en el catálogo.")
    return CATALOG_DB[code_normalized]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

