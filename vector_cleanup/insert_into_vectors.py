from pgvector import Vector
from pgvector.psycopg2 import register_vector
from transformers import CLIPProcessor, CLIPModel
import torch
import psycopg2
import cv2
import numpy as np
from pathlib import Path

# Setup
DB_NAME = "time_traveler"
DB_USER = "postgres"
DB_PASS = "m06Ar14u"
DB_HOST = "192.168.1.201"
DB_PORT = "5432"

TABLE_NAME = "unwanted_frames"
VECTOR_DIM = 512  # CLIP ViT-B/32

device = "cuda" if torch.cuda.is_available() else "cpu"
model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32").to(device)
processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")


def embed_image(image_path: Path) -> np.ndarray:
    image = cv2.cvtColor(cv2.imread(str(image_path)), cv2.COLOR_BGR2RGB)
    inputs = processor(images=image, return_tensors="pt")
    inputs = {k: v.to(device) for k, v in inputs.items()}  # Move tensors to device
    with torch.no_grad():
        embedding = model.get_image_features(**inputs)
        embedding = embedding / embedding.norm(p=2, dim=-1, keepdim=True)
    return embedding.cpu().numpy()[0]


def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE EXTENSION IF NOT EXISTS vector;
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                label TEXT,
                frame_time TEXT,
                video_id TEXT,
                frame_image_path TEXT,
                embedding vector({VECTOR_DIM})
            );
        """)
        conn.commit()


def insert_embedding(conn, label, frame_time, video_id, frame_image_path, embedding):
    embedding = embedding / np.linalg.norm(embedding)
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (label, frame_time, video_id, frame_image_path, embedding)
            VALUES (%s, %s, %s, %s, %s)
        """, (label, frame_time, video_id, str(frame_image_path), Vector(embedding.tolist())))
        conn.commit()


def bulk_insert_from_directory(conn, root_dir: str):
    """
    Walks through a directory and inserts embeddings for all image files found.
    """
    root = Path(root_dir)
    if not root.exists():
        print(f"Directory not found: {root}")
        return

    image_extensions = {'.jpg', '.jpeg', '.png'}
    total_inserted = 0

    for image_path in root.rglob("*"):
        if image_path.suffix.lower() not in image_extensions:
            continue

        try:
            emb = embed_image(image_path)
            # Extract dummy metadata from filename
            label = image_path.stem
            frame_time = "unknown"
            video_id = image_path.parent.name

            insert_embedding(
                conn,
                label=label,
                frame_time=frame_time,
                video_id=video_id,
                frame_image_path=image_path,
                embedding=emb
            )
            print(f"Inserted: {image_path}")
            total_inserted += 1
        except Exception as e:
            print(f"Failed to process {image_path}: {e}")

    print(f"Done. Inserted {total_inserted} embeddings.")


def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
    register_vector(conn)  # Register pgvector adapter for psycopg2
    create_table_if_not_exists(conn)

    # Replace this with your image directory
    image_dir = "./unwanted_frames"
    bulk_insert_from_directory(conn, image_dir)

    conn.close()



if __name__ == "__main__":
    main()
