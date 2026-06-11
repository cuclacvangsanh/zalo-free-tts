import io
import re
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, Response, request, jsonify
import requests

app = Flask(__name__)

# Cấu hình danh sách tham số hợp lệ để Validate dữ liệu đầu vào
VALID_SPEAKERS = ["1", "2", "3", "4", "5", "6"]
VALID_SPEEDS = ["0.8", "0.9", "1.0", "1.1", "1.2"]

def split_text_smart(text, max_chars=500):
    """Cắt văn bản dài thành các đoạn nhỏ < max_chars, không làm đứt câu"""
    sentences = re.split(r'(?<=[.?!,])\s+', text)
    chunks = []
    current_chunk = ""
    
    for sentence in sentences:
        if len(current_chunk) + len(sentence) + 1 <= max_chars:
            current_chunk += (" " if current_chunk else "") + sentence
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            # Nếu bản thân câu đó quá dài, cắt ép buộc theo khoảng trắng
            if len(sentence) > max_chars:
                words = sentence.split(' ')
                sub_chunk = ""
                for word in words:
                    if len(sub_chunk) + len(word) + 1 <= max_chars:
                        sub_chunk += (" " if sub_chunk else "") + word
                    else:
                        chunks.append(sub_chunk.strip())
                        sub_chunk = word
                current_chunk = sub_chunk
            else:
                current_chunk = sentence
                
    if current_chunk:
        chunks.append(current_chunk.strip())
    return chunks

def process_single_chunk(session, text_chunk, speaker_id, speed, chunk_index):
    """Xử lý một đoạn văn bản: Gọi API Zalo -> Đợi m3u8 -> Lấy danh sách link .ts"""
    try:
        # 1. Gọi API Zalo TTS
        api_res = session.post(
            "https://ai.zalo.solutions/api/demo/v1/tts/synthesize",
            headers={"content-type": "application/x-www-form-urlencoded"},
            data={
                "input": text_chunk,
                "speaker_id": speaker_id,
                "speed": speed,
                "dict_id": "0",
                "quality": "0"
            },
            timeout=8
        )
        res_json = api_res.json()
        if res_json.get("error_code") != 0:
            return chunk_index, None
            
        current_url = res_json["data"]["url"]
        
        # 2. Polling nhanh để đọc danh sách file .ts (Tối đa 15 lần * 0.2s = 3s)
        for _ in range(15):
            m3u8_content = session.get(current_url, timeout=5).text
            lines = re.findall(r"^(?!#)(.+)$", m3u8_content, re.MULTILINE)
            if not lines:
                time.sleep(0.2)
                continue
                
            first_line = lines[0].strip()
            if ".m3u8" in first_line:
                current_url = first_line if first_line.startswith("http") else urljoin(current_url, first_line)
                continue
                
            ts_urls = [line.strip() if line.strip().startswith("http") else urljoin(current_url, line.strip()) for line in lines]
            return chunk_index, ts_urls
            
    except Exception as e:
        pass
    return chunk_index, None

def download_ts_shard(session, url, global_index):
    """Tải một file .ts đơn lẻ về dưới dạng bytes"""
    try:
        res = session.get(url, timeout=10)
        if res.status_code == 200:
            return global_index, res.content
    except:
        pass
    return global_index, None


@app.route("/api/tts", methods=["POST", "GET"])
def tts_api():
    # Hỗ trợ lấy tham số từ cả GET (Query) hoặc POST (JSON/Form) để linh hoạt làm API
    if request.method == "POST":
        if request.is_json:
            req_data = request.get_json()
        else:
            req_data = request.form
    else:
        req_data = request.args

    text = req_data.get("text", "").strip()
    speaker_id = str(req_data.get("speaker_id", "1")).strip()
    speed = str(req_data.get("speed", "1.0")).strip()

    # 1. Validation kiểm tra dữ liệu đầu vào
    if not text:
        return jsonify({"error": "Missing 'text' parameter"}), 400
    if len(text) > 20000:
        return jsonify({"error": "Text length exceeds maximum 20,000 characters"}), 400
    if speaker_id not in VALID_SPEAKERS:
        return jsonify({"error": f"Invalid speaker_id. Choose from {VALID_SPEAKERS}"}), 400
    if speed not in VALID_SPEEDS:
        return jsonify({"error": f"Invalid speed. Choose from {VALID_SPEEDS}"}), 400

    # Khởi tạo session và ép header
    session = requests.Session()
    session.headers.update({
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
        'accept': '*/*', 'accept-language': 'en-US,en;q=0.9,vi;q=0.8',
        'origin': 'https://ai.zalo.solutions', 'referer': 'https://ai.zalo.solutions/products/text-to-audio-converter'
    })
    
    # Kích hoạt cookie đồng bộ ban đầu
    try:
        session.get("https://ai.zalo.solutions/products/text-to-audio-converter", timeout=5)
    except Exception:
        return jsonify({"error": "Failed to connect to Zalo Solutions"}), 500

    # 2. Thực hiện cắt chuỗi thông minh thành nhiều phân đoạn
    text_chunks = split_text_smart(text, max_chars=600)
    
    # 3. Chạy đa luồng gửi song song các phân đoạn lên Zalo AI (Max 10 luồng xử lý văn bản)
    chunk_to_ts_mapping = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_single_chunk, session, chunk, speaker_id, speed, idx)
            for idx, chunk in enumerate(text_chunks)
        ]
        for future in futures:
            chunk_idx, ts_list = future.result()
            if ts_list:
                chunk_to_ts_mapping[chunk_idx] = ts_list

    # Sắp xếp lại danh sách các link file .ts theo đúng thứ tự đoạn văn bản gốc
    ordered_ts_urls = []
    for idx in range(len(text_chunks)):
        if idx in chunk_to_ts_mapping:
            ordered_ts_urls.extend(chunk_to_ts_mapping[idx])
        else:
            return jsonify({"error": f"Failed to render segment index {idx+1}"}), 500

    if not ordered_ts_urls:
        return jsonify({"error": "No audio stream generated"}), 500

    # 4. Chạy đa luồng cấp độ 2: Tải đồng loạt tất cả các file .ts cùng một lúc (Max 25 luồng tải file)
    total_shards = len(ordered_ts_urls)
    downloaded_bytes_map = {}
    
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = [
            executor.submit(download_ts_shard, session, url, global_idx)
            for global_idx, url in enumerate(ordered_ts_urls)
        ]
        for future in futures:
            global_idx, content = future.result()
            if content:
                downloaded_bytes_map[global_idx] = content

    # 5. Gộp các mảng bytes tuần tự chuẩn chỉnh vào bộ nhớ RAM
    audio_buffer = io.BytesIO()
    for global_idx in range(total_shards):
        if global_idx in downloaded_bytes_map:
            audio_buffer.write(downloaded_bytes_map[global_idx])
        else:
            return jsonify({"error": f"Data loss at audio fragment {global_idx+1}"}), 500

    audio_buffer.seek(0)

    # 6. Trả file stream Mp3 chất lượng cao về thẳng Client
    return Response(
        audio_buffer.read(),
        mimetype="audio/mp3",
        headers={
            "Content-Disposition": f"attachment; filename=zalo_tts_{int(time.time())}.mp3",
            "Cache-Control": "no-cache"
        }
    )

# Chạy Local development nếu cần test bằng lệnh python api/index.py
if __name__ == "__main__":
    app.run(debug=True, port=5000)
