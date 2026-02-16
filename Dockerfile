# Python 3.11 Slim Version ကို အခြေခံထားမယ်
FROM python:3.11-slim

# OS ကို Update လုပ်ပြီး Node.js နဲ့ NPM ကိုပါ တစ်ခါတည်း သွင်းမယ်
RUN apt-get update && \
    apt-get install -y nodejs npm && \
    rm -rf /var/lib/apt/lists/*

# အလုပ်လုပ်မယ့် Folder နာမည် သတ်မှတ်မယ်
WORKDIR /app

# Python Libraries တွေ အရင် Install လုပ်မယ်
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Project ဖိုင်အားလုံးကို Container ထဲ ကူးထည့်မယ်
COPY . .

# Bot ကို စတင် Run မယ်
CMD ["python", "main.py"]
