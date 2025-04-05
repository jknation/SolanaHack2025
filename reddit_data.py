import praw
import json
import requests
import datetime
from google.colab import auth
from google.cloud import bigquery
from google.cloud import storage
from google.colab import auth

# Google Cloud 인증
auth.authenticate_user()

# BigQuery 클라이언트 초기화
bigquery_client = bigquery.Client()

# Reddit API 인증정보 입력
reddit = praw.Reddit(
    client_id='your_client_id',
    client_secret='your_client_secret',
    user_agent='your_user_agent'
)

# r/memes 서브레딧 접근
subreddit = reddit.subreddit('memes')

# 테이블 정보 설정
table_id = 'solana-meme-coin.solana_meme_2025.reddit_posts'

print(f"데이터를 {table_id}에 삽입합니다...")

# 테이블 스키마 확인
table = bigquery_client.get_table(table_id)
print(f"테이블 스키마:")
for field in table.schema:
    print(f"- {field.name} ({field.field_type})")

# Reddit 데이터 수집
top_posts = []

# 인기 게시물 10개 추출
for submission in subreddit.hot(limit=10):
    # timestamp를 ISO 형식 문자열로 변환
    created_time = datetime.datetime.fromtimestamp(submission.created_utc).isoformat()

    # Submission 객체로부터 추출 가능한 여러 데이터
    post_data = {
        "id": submission.id,
        "title": submission.title,
        "author": str(submission.author),
        "created_utc": created_time,
        "url": submission.url,
        "selftext": submission.selftext,
        "score": submission.score,
        "upvote_ratio": submission.upvote_ratio,
        "num_comments": submission.num_comments,
        "permalink": f"https://reddit.com{submission.permalink}",
        "is_video": submission.is_video,
        "over_18": submission.over_18,
        "spoiler": submission.spoiler,
        "subreddit": str(submission.subreddit),
        "link_flair_text": submission.link_flair_text,
        "link_flair_css_class": submission.link_flair_css_class,
        "gilded": submission.gilded,
        "total_awards_received": submission.total_awards_received,
        "num_crossposts": getattr(submission, "num_crossposts", 0),
        "is_self": submission.is_self,
        "is_original_content": getattr(submission, "is_original_content", False),
        # media 필드 제거됨
    }

    top_posts.append(post_data)

# 데이터 출력
print(f"수집된 포스트 수: {len(top_posts)}")
if top_posts:
    print(f"첫 번째 포스트 ID: {top_posts[0]['id']}, 제목: {top_posts[0]['title']}")

# BigQuery에 데이터 삽입
try:
    errors = bigquery_client.insert_rows_json(table_id, top_posts)
    if errors == []:
        print(f"성공적으로 {len(top_posts)}개의 행이 {table_id}에 추가되었습니다.")
    else:
        print(f"데이터 삽입 중 오류가 발생했습니다:")
        for error in errors:
            print(error)
except Exception as e:
    print(f"데이터 삽입 중 예외가 발생했습니다: {e}")

# Google Cloud 인증
auth.authenticate_user()
print("인증이 완료되었습니다.")

# 프로젝트 ID 설정
project_id = "solana-meme-coin"

# BigQuery 및 Storage 클라이언트 초기화 (프로젝트 ID 명시)
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

print(f"프로젝트 ID: {bigquery_client.project}")
print(f"BigQuery 클라이언트가 성공적으로 초기화되었습니다.")

# GCS 버킷 설정
bucket_name = "meme-data"
try:
    bucket = storage_client.bucket(bucket_name)
    print(f"GCS 버킷 '{bucket_name}'에 연결되었습니다.")
except Exception as e:
    print(f"GCS 버킷 연결 오류: {e}")
    exit(1)

# BigQuery 테이블 정보 설정
source_table_id = 'solana-meme-coin.solana_meme_2025.reddit_posts'
target_table_id = 'solana-meme-coin.solana_meme_2025.image_url'

# 테이블 스키마 조회
try:
    print(f"테이블 '{target_table_id}'의 스키마를 조회합니다...")
    table = bigquery_client.get_table(target_table_id)
    print("기존 테이블 스키마:")
    for field in table.schema:
        print(f"- {field.name} ({field.field_type})")
except Exception as e:
    print(f"테이블 스키마 조회 중 오류 발생: {e}")
    print("테이블이 존재하지 않을 수 있습니다. 새 테이블을 생성합니다.")

    # 테이블 생성
    schema = [
        bigquery.SchemaField("reddit_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gcs_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="NULLABLE")
    ]

    table = bigquery.Table(target_table_id, schema=schema)
    try:
        table = bigquery_client.create_table(table)
        print(f"테이블 '{target_table_id}'가 생성되었습니다.")
    except Exception as e:
        print(f"테이블 생성 중 오류 발생: {e}")
        exit(1)

# 이미지 URL에서 이미지를 다운로드하여 GCS에 업로드하는 함수
def upload_image_to_gcs(image_url, post_id):
    try:
        if not image_url:
            print(f"이미지 URL이 비어있습니다: {post_id}")
            return None

        # 이미지 다운로드
        print(f"이미지 다운로드 시도: {image_url}")
        response = requests.get(image_url, timeout=10)
        if response.status_code != 200:
            print(f"이미지 다운로드 실패: {image_url} (상태 코드: {response.status_code})")
            return None

        # 파일 확장자 확인
        extension = image_url.split('.')[-1].lower()
        if extension not in ['jpg', 'jpeg', 'png', 'gif']:
            print(f"확장자가 인식되지 않아 기본값 jpg로 설정: {image_url}")
            extension = 'jpg'  # 기본값 설정

        # GCS에 저장할 경로 및 파일명 설정
        destination_blob_name = f"memes/{post_id}.{extension}"

        # Content-Type 설정
        if extension in ['jpg', 'jpeg']:
            content_type = 'image/jpeg'
        elif extension == 'png':
            content_type = 'image/png'
        elif extension == 'gif':
            content_type = 'image/gif'
        else:
            content_type = 'application/octet-stream'

        # GCS에 업로드
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(response.content, content_type=content_type)

        # uniform bucket-level access가 활성화된 경우 URL 구성
        public_url = f"https://storage.googleapis.com/{bucket_name}/{destination_blob_name}"

        print(f"이미지 업로드 성공: {public_url}")
        return public_url

    except Exception as e:
        print(f"이미지 업로드 중 오류 발생: {e}")
        return None

# 최근 데이터 10개를 가져오는 쿼리
query = f"""
    SELECT id, title, url
    FROM `{source_table_id}`
    WHERE url IS NOT NULL
    ORDER BY created_utc DESC
    LIMIT 10
"""

print(f"최근 레코드 10개를 {source_table_id}에서 가져오는 중...")
print(f"실행할 쿼리: {query}")

# 쿼리 실행
try:
    print("BigQuery 쿼리 실행 중...")
    query_job = bigquery_client.query(query)
    print(f"쿼리 작업 ID: {query_job.job_id}")
    print("쿼리 결과를 가져오는 중...")
    results = query_job.result()
    rows = list(results)
    print(f"가져온 레코드 수: {len(rows)}")

    if len(rows) == 0:
        print("가져온 데이터가 없습니다. 쿼리 또는 테이블을 확인하세요.")
        exit(1)
except Exception as e:
    print(f"BigQuery 쿼리 실행 중 오류 발생: {e}")
    print("인증 및 프로젝트 설정을 확인하세요.")
    exit(1)

# 이미지 처리 및 BigQuery 업데이트
insert_count = 0
for row in rows:
    post_id = row.id
    url = row.url

    print(f"처리 중: {post_id} - {row.title}")

    # 이미지 URL이 있고 이미지 파일 확장자를 가진 경우에만 처리
    if url and url.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
        # GCS에 이미지 업로드
        gcs_image_url = upload_image_to_gcs(url, post_id)

        if gcs_image_url:
            # image_url 테이블에 데이터 삽입 (post_id 컬럼 제외)
            insert_query = f"""
            INSERT INTO `{target_table_id}` (reddit_image_url, gcs_image_url, processed_at)
            VALUES ('{url}', '{gcs_image_url}', CURRENT_TIMESTAMP())
            """

            try:
                print(f"BigQuery 삽입 쿼리 실행: {post_id}")
                insert_job = bigquery_client.query(insert_query)
                insert_job.result()  # 쿼리 완료 대기
                print(f"BigQuery 삽입 성공: {post_id}")
                insert_count += 1
            except Exception as e:
                print(f"BigQuery 삽입 중 오류 발생: {e}")
        else:
            print(f"GCS 업로드 실패로 BigQuery 삽입 건너뜀: {post_id}")
    else:
        print(f"이미지 URL이 아니거나 지원되지 않는 형식입니다: {url}")

print(f"총 {len(rows)}개 중 {insert_count}개 레코드가 성공적으로 삽입되었습니다.")

# 최종 결과 확인 쿼리
check_query = f"""
SELECT reddit_image_url, gcs_image_url, processed_at
FROM `{target_table_id}`
ORDER BY processed_at DESC
LIMIT 5
"""

try:
    print("\n최종 결과 확인:")
    check_job = bigquery_client.query(check_query)
    check_results = check_job.result()

    for row in check_results:
        print(f"Reddit URL: {row.reddit_image_url}")
        print(f"GCS URL: {row.gcs_image_url}")
        print(f"처리 시간: {row.processed_at}")
        print("---")
except Exception as e:
    print(f"결과 확인 중 오류 발생: {e}")

print("작업이 완료되었습니다.")    