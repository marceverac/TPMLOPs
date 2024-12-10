from fastapi import FastAPI, Depends
from typing import Dict, List, Optional
from datetime import datetime, timedelta, date
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session


engine = create_engine('postgresql+psycopg2://postgres:grupo10mlops@grupo-10-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com:5432/postgres')
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

@app.get('/')
def prueba():
    return {"Funciona": True}


@app.get('/recommendations/{ADV}/{Modelo}')
def recommendation(ADV: str, Modelo: str, db: Session = Depends(get_db)):
    processing_date = date.today().strftime('%Y-%m-%d')
    if Modelo == 'TopCTR':
        query = text(
            """
            SELECT product_id, click, impression, ctr
            FROM top_ctr
            WHERE advertiser_id = :ADV
            AND processing_date = :processing_date
            """
        )
    elif Modelo == 'TopProduct':
        query = text(
            """
            SELECT product_id, views
            FROM top_products
            WHERE advertiser_id = :ADV
            AND processing_date = :processing_date
            """
        )
    else:
        return {'error': 'Modelo no válido - Usar TopCTR o TopProduct'}

    results = db.execute(query, {"ADV": ADV, "processing_date": processing_date}).fetchall()
    if not results:
        return {'error': 'No se encontraron recomendaciones para el Advertiser'}
    
    return {
        "advertiser_id": ADV,
        "model": Modelo,
        "recommendations": [dict(row._mapping) for row in results],
    }


@app.get('/stats')
def stats(db: Session = Depends(get_db)):
    try:
        stats_ctr = db.execute(
            text(
                """
                SELECT 
                    COUNT(DISTINCT advertiser_id) AS total_advertisers,
                    AVG(ctr) AS average_ctr 
                FROM top_ctr
                """
            )
        ).fetchone()

        total_advertisers = stats_ctr._mapping["total_advertisers"] if stats_ctr and stats_ctr._mapping["total_advertisers"] is not None else 0
        average_ctr = stats_ctr._mapping["average_ctr"] if stats_ctr and stats_ctr._mapping["average_ctr"] is not None else 0

        return {
            "cant_advertisers": total_advertisers,
            "average_ctr": average_ctr,
        }
    
    #STATS DE TOPPRODUCTS
    except Exception as e:
        return {"error": str(e)}


#HACER ENDPOINT DE HISTORY
@app.get('/history/{ADV}/')
def history(ADV: str, db: Session = Depends(get_db)):
    last_7_days = [(date.today() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    
    history_by_date = {day: {"top_ctr": [], "top_products": []} for day in last_7_days}

    query_ctr = text(
        """
        SELECT processing_date, product_id, click, impression, ctr
        FROM top_ctr
        WHERE advertiser_id = :ADV
        AND processing_date = :processing_date
        """
    )

    query_products = text(
        """
        SELECT processing_date, product_id, views
        FROM top_products
        WHERE advertiser_id = :ADV
        AND processing_date = :processing_date
        """
    )

    for day in last_7_days:
        results_ctr = db.execute(query_ctr, {"ADV": ADV, "processing_date": day}).fetchall()
        if results_ctr:
            history_by_date[day]["top_ctr"] = [dict(row._mapping) for row in results_ctr]
        
        results_products = db.execute(query_products, {"ADV": ADV, "processing_date": day}).fetchall()
        if results_products:
            history_by_date[day]["top_products"] = [dict(row._mapping) for row in results_products]
    
    return {
        "advertiser_id": ADV,
        "history_last_7_days": history_by_date
    }

@app.get('/db_check')
def db_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "Conectado"}
    except Exception as e:
        return {"error": str(e)}