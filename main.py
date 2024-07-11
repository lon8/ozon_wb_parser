from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    application.include_router(router)
    return application


app = get_application()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8080, log_level='debug', access_log=True)
