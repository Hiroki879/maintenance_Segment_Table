import pandas as pd
from sqlalchemy import create_engine, Column, String, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

# 0.環境変数から接続情報を読み込む
DB_USER = os.getenv("DB_USER", "jun") 
DB_PASSWORD = os.getenv("DB_PASSWORD", "Pub1234Adobe")
DB_HOST = os.getenv("DB_HOST", "157.230.251.229")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "adobepub_dev")

# 1. データベース接続設定
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 2. テーブルに対応するPythonクラス（モデル）を定義
Base = declarative_base()

class Segment(Base):
    __tablename__ = 'Segment'  # DBのテーブル名
    __table_args__ = {'schema': 'master'} # スキーマを指定

    segment_id = Column(String(100), primary_key=True)
    segment_name = Column(String(255))
   
# 3. マスターCSVファイルを読み込む
try:
    df = pd.read_csv('segment_master.csv')
except FileNotFoundError:
    print("エラー: segment_master.csv が見つかりません。")
    exit()

# 4. セッションを開始して同期処理を実行
session = SessionLocal()
try:
    for index, row in df.iterrows():
        # CSVのIDをキーに、DBから既存のセグメントを検索
        segment = session.query(Segment).filter(Segment.segment_id == row['segment_id']).first()

        if segment:
            # --- UPDATE処理 ---
            # 既に存在する場合、名前を更新する（Pythonオブジェクトの属性を書き換えるだけ！）
            print(f"Updating: {row['segment_id']}")
            segment.segment_name = row['segment_name']
           
        else:
            # --- INSERT処理 ---
            # 存在しない場合、新しいオブジェクトを作成してセッションに追加する
            print(f"Inserting: {row['segment_id']}")
            new_segment = Segment(
                segment_id=row['segment_id'],
                segment_name=row['segment_name'],
            )
            session.add(new_segment)
    
    # 5. すべての変更をまとめてデータベースにコミット（書き込み）
    session.commit()
    print("Segmentテーブルの同期が完了しました。")

except Exception as e:
    # エラーが発生した場合、すべての変更をロールバック（キャンセル）
    print(f"エラーが発生しました。変更をロールバックします: {e}")
    session.rollback()

finally:
    # 処理が成功しても失敗しても、必ずセッションを閉じる
    session.close()
