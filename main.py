from crawl.event import Event
from fastapi import FastAPI
from enum import Enum

class Category(str, Enum):
    semua = "semua"
    konser = "konser-dan-pertunjukan"
    seminar = "seminar-dan-talkshow"
    workshop = "workshop"
    kontes = "kontes"
    lain = "lain-lain"
    kuis = "kuis"
    festival = "festival"
    
app = FastAPI(title="event detik.com")

@app.get("/api/v1/detik/event")
def get_data_event(category: Category):
    """
    
    """
    category = category.value
    url = f"https://event.detik.com/kategori/{category}"
    data = Event().get_api(url)
    
    return data
