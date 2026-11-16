from ds1 import server
import uvicorn

def main() -> None:
    app = server.app
    uvicorn.run(app=app)
