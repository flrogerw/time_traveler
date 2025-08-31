import torch
import psycopg2
import numpy as np
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from pgvector.psycopg2 import register_vector

# --- Config ---
IMAGE_PATH = "./unwanted_frames/frame_0094.jpg"
PG_CONN_INFO = "dbname=time_traveler host=192.168.1.201 user=postgres password=m06Ar14u"
SIMILARITY_THRESHOLD = -0.20  # cosine distance

# --- Load CLIP model ---
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(PG_CONN_INFO)
register_vector(conn)
cur = conn.cursor()

# --- Load and encode image ---
image = Image.open(IMAGE_PATH).convert("RGB")
inputs = processor(images=image, return_tensors="pt")

with torch.no_grad():
    embedding = model.get_image_features(**inputs).squeeze().numpy()
    embedding = embedding / np.linalg.norm(embedding)

# --- Search DB ---
cur.execute(
    "SELECT id, embedding <#> %s::vector AS distance FROM unwanted_frames ORDER BY distance ASC LIMIT 1",
    (embedding.tolist(),)
)
result = cur.fetchone()

# --- Output ---
if result:
    matched_id, distance = result
    if distance <= SIMILARITY_THRESHOLD:
        print(f"[MATCH] ID {matched_id} (distance {distance:.4f})")
    else:
        print(f"[NO MATCH] Closest ID {matched_id} (distance {distance:.4f})")
else:
    print("No embeddings found in DB.")

cur.close()
conn.close()
