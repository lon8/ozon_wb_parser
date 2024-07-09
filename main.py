from fastapi import FastAPI
from src.handler import router
from loguru import logger
import sys

# Configure loguru to log to both console and file
logger.remove()  # This removes the default handler
logger.add(sys.stdout, format="{time} {level} {message}", level='DEBUG')
logger.add("logs.log", format="{time} {level} {message}", level='DEBUG')

logger.info("Logger configured")

def get_application() -> FastAPI:
    application = FastAPI()
    application.include_router(router)
    return application


app = get_application()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=8000, log_level='debug', access_log=True)