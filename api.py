from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "Streamlit app cannot run on Vercel. Use Streamlit Cloud."
    }
