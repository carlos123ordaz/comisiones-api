from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from routes.vendedor_routes import router as vendedor_router
from routes.invoice_routes import router as invoice_router
from routes.invoice_detail_routes import router as invoice_detail_router
from routes.resumen_routes import router as resumen_router
from routes.auth_routes import router as auth_router
from config.database import client

app = FastAPI(title="API de Dashboard de Ventas")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(auth_router)
app.include_router(vendedor_router)
app.include_router(invoice_router)
app.include_router(invoice_detail_router)
app.include_router(resumen_router)


@app.get("/")
def read_root():
    """Endpoint raíz"""
    return {
        "message": "API de Dashboard con MongoDB funcionando correctamente",
        "version": "2.0",
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    """Verifica el estado de salud de la API"""
    try:
        client.admin.command('ping')
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error de conexión con MongoDB: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
