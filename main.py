import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import sqlite3
import logging
import threading
import re
import sys
import atexit
import requests
import random
import string
import json
import ast
from flask import Flask
from threading import Thread

# --- Flask Keep Alive ---
app = Flask('')

@app.route('/')
def home():
    return "☁️ CLOUD - Premium Hosting Environment"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("☁️ Cloud Keep-Alive server started.")
# --- End Flask Keep Alive ---

# --- Environment Variables Configuration ---
# Railway/Heroku dashboard ကနေ ထည့်ပေးရမယ့် variables
BOT_TOKEN = os.environ.get('BOT_TOKEN')
OWNER_ID = os.environ.get('OWNER_ID')
ADMIN_ID = os.environ.get('ADMIN_ID')
YOUR_USERNAME = os.environ.get('YOUR_USERNAME')

# Environment variables တွေ ပြည့်စုံမှုရှိမရှိ စစ်ဆေးခြင်း
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable is not set!")
if not OWNER_ID:
    raise ValueError("❌ OWNER_ID environment variable is not set!")
if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID environment variable is not set!")
if not YOUR_USERNAME:
    raise ValueError("❌ YOUR_USERNAME environment variable is not set!")

# Convert IDs to integers
try:
    OWNER_ID = int(OWNER_ID)
    ADMIN_ID = int(ADMIN_ID)
except ValueError:
    raise ValueError("❌ OWNER_ID and ADMIN_ID must be valid integers!")

# 1. API ဖြင့် Member စစ်ရန် ID များ (Int အနေနဲ့ထားပါ)
FORCE_CHANNEL_ID = -1002756417115 # Channel ရဲ့ ID အမှန် ပြောင်းထည့်ပါ
FORCE_GROUP_ID = -1002756417115   # Group ရဲ့ ID အမှန် ပြောင်းထည့်ပါ

# 2. User ကို ပြသမည့် Invite Link များ
FORCE_CHANNEL_LINK = 'https://t.me/+NLb-9NFUSiY1YjVl' # သင့် Channel ရဲ့ Invite Link
FORCE_GROUP_LINK = 'https://t.me/+KT3SAWDdC-MxNjJl'     # သင့် Group ရဲ့ Invite Link
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'kelvin_uploads')
KELVIN_DIR = os.path.join(BASE_DIR, 'kelvin_data')
DATABASE_PATH = os.path.join(KELVIN_DIR, 'kelvin_host.db')

# File upload limits
FREE_USER_LIMIT = 1
PREMIUM_USER_LIMIT = 999
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Security scan data
user_warnings = {}
MAX_WARNINGS = 3

os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(KELVIN_DIR, exist_ok=True)

# ✅ ဒီနေရာမှာ BOT_TOKEN ကိုသုံးရမယ် (TOKEN မဟုတ်ဘူး)
bot = telebot.TeleBot(BOT_TOKEN, threaded=True, num_threads=10)

bot_scripts = {}
user_subscriptions = {}
user_files = {}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False
force_join_enabled = True
broadcast_messages = {}

# Security scan data
security_scans = {
    'total_scans': 0,
    'threats_found': 0,
    'high_risk_files': 0,
    'blocked_files': 0
}

# --- Security Scanner ---
SUSPICIOUS_PATTERNS = {
    'source_reading': [
        r'kelvinhost\.py',
        r'open.*kelvinhost',
        r'read.*kelvinhost',
        r'import.*kelvinhost',
        r'from.*kelvinhost',
        r'__file__.*kelvinhost',
        r'os\.path\.dirname.*kelvinhost',
        r'\.\./kelvinhost',
        r'\./kelvinhost',
        r'/kelvinhost\.py',
        r'kelvin_host\.py',
        r'this.*bot.*source',
        r'host.*bot.*code',
    ],
    'file_exfiltration': [
        r'telegram\.send_document',
        r'send_document',
        r'upload.*file',
        r'export.*file',
        r'copy.*file',
        r'shutil\.copy',
        r'os\.system.*cp',
        r'wget.*\.py',
        r'curl.*\.py',
        r'requests\.post.*file',
        r'base64.*encode.*file',
        r'send.*file.*telegram',
        r'forward.*file',
        r'download.*file',
    ],
    'directory_traversal': [
        r'os\.listdir',
        r'os\.walk',
        r'glob\.glob',
        r'Path\(.*\)\.rglob',
        r'find.*\.py',
        r'scan.*directory',
        r'explore.*files',
        r'get.*all.*files',
        r'search.*files',
        r'enumerate.*files',
    ],
    'sensitive_access': [
        r'DATABASE_PATH',
        r'UPLOAD_BOTS_DIR',
        r'KELVIN_DIR',
        r'user_files',
        r'user_subscriptions',
    ],
    'obfuscation': [
        r'exec\(',
        r'eval\(',
        r'__import__\(',
        r'compile\(',
        r'base64\.b64decode',
        r'codecs\.decode',
        r'getattr.*__',
        r'setattr.*__',
        r'bytearray.*decode',
        r'str.*decode',
    ],
    'backdoor': [
        r'socket\.connect',
        r'bind.*port',
        r'listen.*port',
        r'accept\(\)',
        r'shell.*true',
        r'pty\.spawn',
        r'subprocess\.Popen.*shell',
        r'os\.system',
        r'os\.popen',
        r'backconnect',
        r'reverse.*shell',
    ]
}

SUSPICIOUS_IMPORTS = [
    'os', 'sys', 'subprocess', 'pathlib',
    'zipfile', 'tempfile', 'requests', 'base64',
    'codecs', 'pickle', 'marshal', 'ctypes',
    'pty', 'telnetlib', 'ftplib', 'smtplib'
]

# Supported files
SUPPORTED_EXTENSIONS = {
    '.py': 'Python', '.java': 'Java', '.html': 'HTML', '.htm': 'HTML',
    '.js': 'JavaScript', '.css': 'CSS', '.txt': 'Text', '.json': 'JSON',
    '.xml': 'XML', '.php': 'PHP', '.c': 'C', '.cpp': 'C++', '.cs': 'C#',
    '.rb': 'Ruby', '.go': 'Go', '.rs': 'Rust', '.md': 'Markdown',
    '.yaml': 'YAML', '.yml': 'YAML', '.sql': 'SQL', '.sh': 'Shell',
    '.bat': 'Batch', '.ps1': 'PowerShell', '.r': 'R', '.swift': 'Swift',
    '.kt': 'Kotlin', '.scala': 'Scala', '.pl': 'Perl', '.lua': 'Lua',
    '.ts': 'TypeScript', '.jsx': 'React JSX', '.tsx': 'React TSX',
    '.vue': 'Vue', '.svelte': 'Svelte', '.dart': 'Dart', '.scss': 'SCSS',
    '.less': 'Less', '.styl': 'Stylus', '.coffee': 'CoffeeScript'
}

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_db():
    """initialize the database with required tables"""
    logger.info(f"🛢️ Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        c.execute('''create table if not exists users
                     (user_id integer primary key, 
                      username text, 
                      first_name text, 
                      last_name text, 
                      join_date timestamp default current_timestamp,
                      verified integer default 0,
                      key_used text,
                      key_used_date timestamp)''')
        
        c.execute('''create table if not exists subscriptions
                     (user_id integer primary key, expiry text, 
                      file_limit integer default 999,
                      redeemed_date timestamp default current_timestamp)''')
        
        c.execute('''create table if not exists user_files
                     (file_id integer primary key autoincrement,
                      user_id integer,
                      username text,
                      chat_id integer,
                      file_name text, 
                      file_type text, 
                      file_path text,
                      original_filename text,
                      file_size integer,
                      upload_date timestamp default current_timestamp,
                      is_active integer default 1,
                      is_pending integer default 0,
                      FOREIGN KEY (user_id) REFERENCES users(user_id))''')
        
        c.execute('''create table if not exists active_users
                     (user_id integer primary key)''')
        
        c.execute('''create table if not exists admins
                     (user_id integer primary key)''')
        
        c.execute('''create table if not exists subscription_keys
                     (key_value text primary key,
                      created_by integer,
                      created_date timestamp default current_timestamp,
                      days_valid integer,
                      max_uses integer default 1,
                      used_count integer default 0,
                      file_limit integer default 999,
                      is_active integer default 1,
                      used_by_user integer,
                      used_date timestamp)''')
        
        c.execute('''create table if not exists key_usage
                     (key_value text, user_id integer, used_date timestamp default current_timestamp,
                      primary key (key_value, user_id))''')
        
        c.execute('''create table if not exists bot_settings
                     (setting_key text primary key, setting_value text)''')
        
        c.execute('''create table if not exists banned_users
                     (user_id integer primary key,
                      banned_by integer,
                      ban_date timestamp default current_timestamp,
                      reason text)''')
        
        c.execute('''create table if not exists security_logs
                     (log_id integer primary key autoincrement,
                      user_id integer,
                      username text,
                      file_name text,
                      threat_count integer,
                      risk_level text,
                      action_taken text,
                      log_date timestamp default current_timestamp)''')
        
        c.execute('insert or ignore into bot_settings (setting_key, setting_value) values (?, ?)', 
                 ('free_user_limit', str(FREE_USER_LIMIT)))
        c.execute('insert or ignore into bot_settings (setting_key, setting_value) values (?, ?)', 
                 ('force_join_enabled', '1'))
        
        c.execute('insert or ignore into admins (user_id) values (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
            c.execute('insert or ignore into admins (user_id) values (?)', (ADMIN_ID,))
        
        conn.commit()
        conn.close()
        logger.info("✅ Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """load data from database into memory"""
    logger.info("📥 Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        c.execute('select user_id, expiry, file_limit from subscriptions')
        for user_id, expiry, file_limit in c.fetchall():
            try:
                user_subscriptions[user_id] = {
                    'expiry': datetime.fromisoformat(expiry),
                    'file_limit': file_limit if file_limit else 999
                }
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Only load non-pending files
        c.execute('select user_id, file_name, file_type, file_path from user_files where is_pending = 0')
        for user_id, file_name, file_type, file_path in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type, file_path))

        c.execute('select user_id from active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        c.execute('select user_id from admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())

        c.execute('select setting_key, setting_value from bot_settings')
        for key, value in c.fetchall():
            if key == 'free_user_limit':
                global FREE_USER_LIMIT
                FREE_USER_LIMIT = int(value) if value.isdigit() else 1
            elif key == 'force_join_enabled':
                global force_join_enabled
                force_join_enabled = value == '1'

        conn.close()
        logger.info(f"📊 Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

init_db()
load_data()

# --- Security Scanner Functions ---
def scan_file_for_threats(file_path, user_id, username, file_name):
    """Scan uploaded file for security threats"""
    threats_found = []
    file_content = ""
    
    try:
        security_scans['total_scans'] += 1
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            file_content = f.read()
        
        if not file_content.strip():
            return threats_found
        
        import_lines = []
        lines = file_content.split('\n')
        for i, line in enumerate(lines, 1):
            line_lower = line.lower()
            if any(imp in line_lower for imp in ['import ', 'from ']):
                for sus_import in SUSPICIOUS_IMPORTS:
                    if sus_import in line_lower and 'telebot' not in line_lower:
                        import_lines.append(f"Line {i}: {line.strip()}")
            
            for category, patterns in SUSPICIOUS_PATTERNS.items():
                for pattern in patterns:
                    if re.search(pattern, line_lower, re.IGNORECASE):
                        threat = {
                            'category': category,
                            'line': i,
                            'content': line.strip()[:100],
                            'pattern': pattern
                        }
                        threats_found.append(threat)
        
        if import_lines:
            threats_found.append({
                'category': 'suspicious_imports',
                'line': 0,
                'content': '; '.join(import_lines)[:200],
                'pattern': 'multiple_suspicious_imports'
            })
        
        if file_path.endswith('.py'):
            try:
                tree = ast.parse(file_content)
                threats_found.extend(analyze_ast(tree, file_path))
            except SyntaxError:
                pass 
        
        if threats_found:
            security_scans['threats_found'] += 1
        
    except Exception as e:
        logger.error(f"❌ Error scanning file {file_path}: {e}")
    
    return threats_found

def analyze_ast(tree, file_path):
    """Analyze Python AST for suspicious patterns"""
    threats = []
    
    class ThreatVisitor(ast.NodeVisitor):
        def visit_Call(self, node):
            try:
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                    dangerous_calls = ['exec', 'eval', '__import__', 'compile', 
                                     'system', 'popen', 'call', 'run', 'spawn']
                    
                    if func_name in dangerous_calls:
                        threats.append({
                            'category': 'dangerous_call',
                            'line': node.lineno if hasattr(node, 'lineno') else 0,
                            'content': f"{func_name}() called",
                            'pattern': f'dangerous_function_{func_name}'
                        })
                
                if isinstance(node.func, ast.Attribute):
                    if node.func.attr in ['open', 'read', 'write', 'copy', 'move', 'remove']:
                        for arg in node.args:
                            if isinstance(arg, ast.Str):
                                if 'kelvinhost' in arg.s.lower() or 'kelvin_host' in arg.s.lower():
                                    threats.append({
                                        'category': 'source_access',
                                        'line': node.lineno if hasattr(node, 'lineno') else 0,
                                        'content': f"Accessing: {arg.s}",
                                        'pattern': 'accessing_kelvinhost'
                                    })
            except:
                pass
            self.generic_visit(node)
        
        def visit_Import(self, node):
            for alias in node.names:
                if alias.name in SUSPICIOUS_IMPORTS:
                    threats.append({
                        'category': 'suspicious_import',
                        'line': node.lineno if hasattr(node, 'lineno') else 0,
                        'content': f"import {alias.name}",
                        'pattern': f'import_{alias.name}'
                    })
            self.generic_visit(node)
        
        def visit_ImportFrom(self, node):
            if node.module and node.module in SUSPICIOUS_IMPORTS:
                threats.append({
                    'category': 'suspicious_import',
                    'line': node.lineno if hasattr(node, 'lineno') else 0,
                    'content': f"from {node.module} import ...",
                    'pattern': f'from_{node.module}'
                })
            self.generic_visit(node)
    
    try:
        visitor = ThreatVisitor()
        visitor.visit(tree)
    except:
        pass
    
    return threats

def generate_threat_report(threats, user_id, username, file_name, file_path):
    """Generate a detailed threat report"""
    if not threats:
        return None
    
    report = {
        'user_id': user_id,
        'username': username or 'Unknown',
        'file_name': file_name,
        'file_path': file_path,
        'scan_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'threat_count': len(threats),
        'threats_by_category': {},
        'high_risk': False,
        'critical_risk': False
    }
    
    for threat in threats:
        category = threat['category']
        if category not in report['threats_by_category']:
            report['threats_by_category'][category] = []
        report['threats_by_category'][category].append(threat)
        
        high_risk_categories = ['source_reading', 'dangerous_call', 'source_access', 'backdoor']
        critical_categories = ['source_reading', 'backdoor']
        
        if category in high_risk_categories:
            report['high_risk'] = True
        if category in critical_categories:
            report['critical_risk'] = True
    
    return report

def send_threat_alert_to_owner(report):
    """Send threat alert to owner with action buttons"""
    if not report:
        return
    
    user_id = report['user_id']
    username = report['username'] or 'Unknown'
    file_name = report['file_name']
    threat_count = report['threat_count']
    high_risk = report['high_risk']
    critical_risk = report['critical_risk']
    
    username_clean = username.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')
    file_name_clean = file_name.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')
    
    risk_level = '🔴 CRITICAL RISK' if critical_risk else '🟠 HIGH RISK' if high_risk else '🟡 MEDIUM RISK'
    
    alert_text = f"""
🚨 *SECURITY ALERT - MALICIOUS FILE DETECTED* 🚨

*User Information:*
• ID: `{user_id}`
• Username: {username_clean}
• File: `{file_name_clean}`
• Time: {report['scan_time']}
• Risk Level: {risk_level}

*Threat Analysis:*
• Total Threats: {threat_count}
• Categories Found: {', '.join(report['threats_by_category'].keys())}
• Critical Patterns: {'YES' if critical_risk else 'NO'}

*Top Threats Found:*
"""
    
    threat_details = ""
    count = 0
    for category, threats in report['threats_by_category'].items():
        for threat in threats[:1]: 
            if count < 3: 
                threat_content = threat['content'][:60].replace('`', "'").replace('*', '').replace('_', '')
                threat_details += f"• {category.upper()}: Line {threat['line']}\n"
                threat_details += f"  `{threat_content}...`\n"
                count += 1
    
    alert_text += threat_details
    
    if critical_risk:
        alert_text += f"""
        
*CRITICAL THREAT DETECTED*
*AUTO-BLOCKED* - File deleted and user restricted

Required manual review:
"""
    elif high_risk:
        alert_text += f"""
        
*HIGH RISK THREAT*
Immediate action recommended

Choose action below:
"""
    else:
        alert_text += f"""
        
*SUSPICIOUS PATTERNS*
Review recommended

Choose action below:
"""
    
    markup = types.InlineKeyboardMarkup(row_width=2)
    
    if critical_risk:
        markup.add(
            types.InlineKeyboardButton("🚫 BAN USER", callback_data=f'security_ban_{user_id}_{file_name_clean}'),
            types.InlineKeyboardButton("🔒 BLOCK UPLOADS", callback_data=f'security_block_{user_id}_{file_name_clean}')
        )
    elif high_risk:
        markup.add(
            types.InlineKeyboardButton("🚫 BAN USER", callback_data=f'security_ban_{user_id}_{file_name_clean}'),
            types.InlineKeyboardButton("⚠️ WARN USER", callback_data=f'security_warn_{user_id}_{file_name_clean}')
        )
    else:
        markup.add(
            types.InlineKeyboardButton("⚠️ WARN USER", callback_data=f'security_warn_{user_id}_{file_name_clean}'),
            types.InlineKeyboardButton("👁️ IGNORE", callback_data=f'security_ignore_{user_id}_{file_name_clean}')
        )
    
    markup.add(
        types.InlineKeyboardButton("🗑️ DELETE FILE", callback_data=f'security_delete_{user_id}_{file_name_clean}'),
        types.InlineKeyboardButton("📋 DETAILS", callback_data=f'security_report_{user_id}_{file_name_clean}')
    )
    
    try:
        bot.send_message(OWNER_ID, alert_text, reply_markup=markup, parse_mode=None)
        
        if not critical_risk:
            try:
                with open(report['file_path'], 'rb') as f:
                    bot.send_document(
                        OWNER_ID,
                        f,
                        caption=f"🚨 Suspicious file: {file_name_clean}\nUser: {username_clean} ({user_id})\nRisk: {risk_level}"
                    )
            except:
                pass
        
        log_security_event(user_id, username, file_name, threat_count, 
                          'critical' if critical_risk else 'high' if high_risk else 'medium',
                          'alerted')
        
        logger.warning(f"⚠️ Security alert sent for user {user_id}, file {file_name}, risk: {risk_level}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send security alert: {e}")
        try:
            simple_alert = f"""
🚨 SECURITY ALERT 🚨

User ID: {user_id}
Username: {username}
File: {file_name}
Risk: {risk_level}
Threats: {threat_count}

Action required.
            """
            bot.send_message(OWNER_ID, simple_alert, reply_markup=markup)
        except Exception as e2:
            logger.error(f"❌ Failed to send fallback alert: {e2}")

def log_security_event(user_id, username, file_name, threat_count, risk_level, action_taken):
    """Log security event to database"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''insert into security_logs 
                     (user_id, username, file_name, threat_count, risk_level, action_taken)
                     values (?, ?, ?, ?, ?, ?)''',
                 (user_id, username, file_name, threat_count, risk_level, action_taken))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Error logging security event: {e}")
    finally:
        conn.close()

def is_user_banned(user_id):
    """Check if user is banned"""
    if user_id in admin_ids or user_id == OWNER_ID:
        return False
    
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('select user_id from banned_users where user_id = ?', (user_id,))
        return c.fetchone() is not None
    finally:
        conn.close()

def check_force_join(user_id):
    """check if user is member of required Channel and Group"""
    if user_id in admin_ids:
        return True
    
    if not force_join_enabled:
        return True
    
    if is_user_banned(user_id):
        return False  

    try:
        # Group Member ဖြစ်/မဖြစ် စစ်ခြင်း
        group_member = bot.get_chat_member(FORCE_GROUP_ID, user_id)  
        if group_member.status not in ['member', 'administrator', 'creator']:
            return False
            
        # (တကယ်လို့ Channel ကိုပါ မဖြစ်မနေ Join ခိုင်းချင်ရင် အောက်က ၃ ကြောင်းကို အရှေ့က # ဖြုတ်လိုက်ပါ)
        # channel_member = bot.get_chat_member(FORCE_CHANNEL_ID, user_id)
        # if channel_member.status not in ['member', 'administrator', 'creator']:
        #     return False
        
        return True
    except Exception as e:
        logger.error(f"❌ Error checking membership for user {user_id}: {e}")
        return False


def create_force_join_message():
    """create force join message with modern UI"""
    return f"""
🔒 *ACCESS RESTRICTED* 🔒

👋 **Welcome! To access KELVIN Vps Host, please join our community:**

🌐 **Official Channel:** [Join Channel]({FORCE_CHANNEL_LINK})
👥 **Community Group:** [Join Group]({FORCE_GROUP_LINK})

---
📋 **Instructions:**
1. Tap the buttons below to join.
2. Wait a few seconds for Telegram to update.
3. Tap "✅ Verify Access".
4. Enjoy unlimited cloud hosting!
    """

def create_force_join_keyboard():
    """create force join keyboard with modern buttons"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    # URL နေရာမှာ Invite Link တိုက်ရိုက်ထည့်ပါမည်
    markup.add(types.InlineKeyboardButton("🌐 Join Channel", url=FORCE_CHANNEL_LINK))
    markup.add(types.InlineKeyboardButton("👥 Join Group", url=FORCE_GROUP_LINK))
    markup.add(types.InlineKeyboardButton("✅ Verify Access", callback_data='check_membership'))
    
    return markup


def mark_user_verified(user_id, verified=True):
    """mark user as verified in database"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('update users set verified = ? where user_id = ?', 
                 (1 if verified else 0, user_id))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Error marking user verified: {e}")
    finally:
        conn.close()

def is_user_verified(user_id):
    """check if user is verified in database"""
    if user_id in admin_ids:
        return True
    
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('select verified from users where user_id = ?', (user_id,))
        result = c.fetchone()
        return result and result[0] == 1
    except Exception as e:
        logger.error(f"❌ Error checking user verification: {e}")
        return False
    finally:
        conn.close()

def get_user_folder(user_id):
    """get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """get the file upload limit for a user"""
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    
    if is_premium_user(user_id):
        subscription_info = user_subscriptions.get(user_id, {})
        return subscription_info.get('file_limit', PREMIUM_USER_LIMIT)
    
    return FREE_USER_LIMIT  

def get_user_file_count(user_id):
    """get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_premium_user(user_id):
    """check if user has active subscription"""
    if user_id in user_subscriptions:
        expiry = user_subscriptions[user_id]['expiry']
        return expiry > datetime.now()
    return False

def get_user_status(user_id):
    """get user status with modern emojis"""
    if user_id == OWNER_ID: return "👑 System Owner"
    if user_id in admin_ids: return "🛡️ Administrator"
    if is_premium_user(user_id): return "💎 Premium"
    return "👤 Standard User"

def get_premium_users_details():
    """get detailed information about premium users"""
    premium_users = []
    for user_id in active_users:
        if is_premium_user(user_id):
            try:
                chat = bot.get_chat(user_id)
                user_files_list = user_files.get(user_id, [])
                running_files = sum(1 for file_name, _, _ in user_files_list if is_bot_running(user_id, file_name))
                subscription_info = user_subscriptions.get(user_id, {})
                file_limit = subscription_info.get('file_limit', PREMIUM_USER_LIMIT)
                
                premium_users.append({
                    'user_id': user_id,
                    'first_name': chat.first_name,
                    'username': chat.username,
                    'file_count': len(user_files_list),
                    'file_limit': file_limit,
                    'running_files': running_files,
                    'expiry': subscription_info['expiry']
                })
            except Exception as e:
                logger.error(f"❌ Error getting user details for {user_id}: {e}")
    
    return premium_users

def generate_subscription_key(days, max_uses=1, file_limit=999, created_by=None):
    """generate subscription key with 1-key 1-user enforcement"""
    part1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    part2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    key = f"KELVIN-{part1}-{part2}"
    
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('''insert into subscription_keys 
                 (key_value, days_valid, max_uses, file_limit, created_by) 
                 values (?, ?, ?, ?, ?)''',
              (key, days, max_uses, file_limit, created_by))
    conn.commit()
    conn.close()
    
    return key

def redeem_subscription_key(key_value, user_id):
    """redeem subscription key - one key per user"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    
    try:
        c.execute('''select days_valid, max_uses, used_count, file_limit, is_active, used_by_user
                     from subscription_keys where key_value = ?''', (key_value,))
        key_data = c.fetchone()
        
        if not key_data:
            return False, "❌ Invalid Key"
        
        days_valid, max_uses, used_count, file_limit, is_active, used_by_user = key_data
        
        if is_active != 1:
            return False, "❌ Key Inactive"
        
        if used_count >= max_uses:
            return False, f"❌ Key Already Used ({used_count}/{max_uses} uses)"
        
        if used_by_user and used_by_user == user_id:
            return False, "❌ You already used this key"
        
        c.execute('''select key_used from users where user_id = ? and 
                     key_used is not null''', (user_id,))
        user_key = c.fetchone()
        
        if user_key:
            return False, "❌ You already have an active key"
        
        current_expiry = user_subscriptions.get(user_id, {}).get('expiry', datetime.now())
        if current_expiry < datetime.now():
            current_expiry = datetime.now()
        
        new_expiry = current_expiry + timedelta(days=days_valid)
        
        save_subscription(user_id, new_expiry, file_limit)
        
        current_time = datetime.now().isoformat()
        c.execute('''update subscription_keys 
                     set used_count = used_count + 1,
                         used_by_user = ?,
                         used_date = ?
                     where key_value = ?''',
                  (user_id, current_time, key_value))
        
        c.execute('''update users 
                     set key_used = ?,
                         key_used_date = ?
                     where user_id = ?''',
                  (key_value, current_time, user_id))
        
        conn.commit()

        try:
            user_info = bot.get_chat(user_id)
            user_mention = f"[{user_info.first_name}](tg://user?id={user_id})" if user_info.first_name else f"User {user_id}"
    
            admin_msg = f"""
💳 **NEW PREMIUM ACTIVATION**

👤 **User:**
├─ ID: `{user_id}`
├─ Name: {user_mention}
├─ Username: @{user_info.username if user_info.username else 'N/A'}
└─ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🔑 **Key Details:**
├─ Key: `{key_value}`
├─ Duration: {days_valid} Days
├─ Files: {file_limit} Files
├─ Uses: {used_count + 1}/{max_uses}
└─ Expires: {new_expiry.strftime('%Y-%m-%d %H:%M:%S')}
            """
            bot.send_message(OWNER_ID, admin_msg, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"❌ Failed to notify admin: {e}")    
        
        return True, f"""
✨ **PREMIUM ACTIVATED SUCCESSFULLY!** ✨

🔑 **Key:** `{key_value}`
👤 **Assigned to:** You
📅 **Duration:** {days_valid} Days
🗃 **File Limit:** {file_limit} Files
⏰ **Start:** {datetime.now().strftime('%Y-%m-%d')}
⏳ **End:** {new_expiry.strftime('%Y-%m-%d')}

📝 **Note:**
• This key is now linked to your account.
• It cannot be used by anyone else.
• You cannot use another key.
        """
    
    except Exception as e:
        return False, f"❌ Error: {str(e)}"
    finally:
        conn.close()

def get_all_subscription_keys():
    """get all subscription keys with details"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('select key_value, days_valid, max_uses, used_count, file_limit, created_date from subscription_keys order by created_date desc')
    keys = c.fetchall()
    conn.close()
    return keys

def delete_subscription_key(key_value):
    """delete subscription key and remove premium status from users"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    
    c.execute('select user_id from key_usage where key_value = ?', (key_value,))
    users_affected = c.fetchall()
    
    for (user_id,) in users_affected:
        if user_id in user_subscriptions:
            del user_subscriptions[user_id]
        c.execute('delete from subscriptions where user_id = ?', (user_id,))
        
        try:
            bot.send_message(user_id, "⚠️ **Your Premium Access has been Revoked**\n\nThe key used has been deactivated.")
        except Exception as e:
            logger.error(f"❌ Failed to notify user {user_id}: {e}")
    
    c.execute('delete from subscription_keys where key_value = ?', (key_value,))
    c.execute('delete from key_usage where key_value = ?', (key_value,))
    conn.commit()
    conn.close()

def update_file_limit(new_limit):
    """update free user file limit"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('insert or replace into bot_settings (setting_key, setting_value) values (?, ?)', 
              ('free_user_limit', str(new_limit)))
    conn.commit()
    conn.close()
    
    global FREE_USER_LIMIT
    FREE_USER_LIMIT = new_limit

def update_force_join_status(enabled):
    """update force join status"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('insert or replace into bot_settings (setting_key, setting_value) values (?, ?)', 
              ('force_join_enabled', '1' if enabled else '0'))
    conn.commit()
    conn.close()
    
    global force_join_enabled
    force_join_enabled = enabled

def get_bot_statistics():
    """get comprehensive bot statistics"""
    total_users = len(active_users)
    total_files = sum(len(files) for files in user_files.values())
    
    active_files = 0
    for script_key in bot_scripts:
        if is_bot_running(int(script_key.split('_')[0]), bot_scripts[script_key]['file_name']):
            active_files += 1
    
    premium_users = sum(1 for user_id in active_users if is_premium_user(user_id))
    
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('select count(*) from security_logs')
    security_alerts = c.fetchone()[0]
    c.execute('select count(*) from banned_users')
    banned_users = c.fetchone()[0]
    conn.close()
    
    return {
        'total_users': total_users,
        'total_files': total_files,
        'active_files': active_files,
        'premium_users': premium_users,
        'security_alerts': security_alerts,
        'banned_users': banned_users
    }

def get_all_users_details():
    """get details of all bot users"""
    users_list = []
    for user_id in active_users:
        try:
            chat = bot.get_chat(user_id)
            users_list.append({
                'user_id': user_id,
                'first_name': chat.first_name,
                'username': chat.username,
                'is_premium': is_premium_user(user_id)
            })
        except:
            users_list.append({
                'user_id': user_id,
                'first_name': 'Unknown',
                'username': 'Unknown',
                'is_premium': is_premium_user(user_id)
            })
    return users_list

def get_all_admins():
    """get all admin IDs from database"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    c.execute('select user_id from admins')
    admins = [row[0] for row in c.fetchall()]
    conn.close()
    return admins

def add_admin_to_db(admin_id):
    """add admin to database"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('insert or ignore into admins (user_id) values (?)', (admin_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error adding admin: {e}")
        return False
    finally:
        conn.close()

def remove_admin_from_db(admin_id):
    """remove admin from database"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('delete from admins where user_id = ?', (admin_id,))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"❌ Error removing admin: {e}")
        return False
    finally:
        conn.close()

def is_bot_running(script_owner_id, file_name):
    """check if a bot script is currently running"""
    script_key = f"{script_owner_id}_{file_name}"
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False
    return False

def kill_process_tree(process_info):
    """kill a process and all its children"""
    try:
        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
            pid = process.pid
            try:
                parent = psutil.Process(pid)
                
                try:
                    parent.terminate()
                    parent.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    pass
                
                try:
                    if parent.is_running():
                        parent.kill()
                        parent.wait(timeout=3)
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    pass
                
                try:
                    children = parent.children(recursive=True)
                    for child in children:
                        try:
                            child.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    
                    time.sleep(1)
                    
                    for child in children:
                        try:
                            if child.is_running():
                                child.kill()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
                
            except psutil.NoSuchProcess:
                pass
            
            try:
                if process.poll() is None:
                    process.terminate()
                    time.sleep(2)
                    if process.poll() is None:
                        process.kill()
            except:
                pass
            
            if process_info.get('log_file'):
                try:
                    process_info['log_file'].close()
                except:
                    pass
                
    except Exception as e:
        logger.error(f"❌ Error killing process: {e}")

def force_cleanup_process(process_info):
    """Force cleanup of a process with multiple attempts"""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            kill_process_tree(process_info)
            
            if process_info.get('process'):
                try:
                    pid = process_info['process'].pid
                    psutil.Process(pid)
                    time.sleep(1)  
                    continue
                except psutil.NoSuchProcess:
                    return True  
            
            return True
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to kill process: {e}")
            time.sleep(1)
    
    return False 

def cleanup_user_processes(user_id):
    """Clean up all processes for a specific user"""
    keys_to_remove = []
    for script_key, process_info in list(bot_scripts.items()):
        if script_key.startswith(f"{user_id}_"):
            if force_cleanup_process(process_info):
                keys_to_remove.append(script_key)
    
    for key in keys_to_remove:
        if key in bot_scripts:
            del bot_scripts[key]
    
    return len(keys_to_remove)

TELEGRAM_MODULES = {
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'requests': 'requests',
    'bs4': 'beautifulsoup4',
    'pillow': 'Pillow',
    'cv2': 'opencv-python',
    'yaml': 'PyYAML',
    'dotenv': 'python-dotenv',
    'dateutil': 'python-dateutil',
    'pandas': 'pandas',
    'numpy': 'numpy',
    'flask': 'Flask',
    'django': 'Django',
    'sqlalchemy': 'SQLAlchemy',
    'psutil': 'psutil',
    'asyncio': None, 'json': None, 'datetime': None, 'os': None, 'sys': None, 're': None,
    'time': None, 'math': None, 'random': None, 'logging': None, 'threading': None,
    'subprocess': None, 'zipfile': None, 'tempfile': None, 'shutil': None, 'sqlite3': None
}

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name) 
    if package_name is None: 
        logger.info(f"📦 Module '{module_name}' is core. Skipping pip install.")
        return False 
    try:
        try:
            bot.send_message(message.from_user.id, f"🔧 Installing `{package_name}`...", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"❌ Failed to send install message: {e}")
            return False
            
        command = [sys.executable, '-m', 'pip', 'install', package_name, '--timeout', '60', '--retries', '3']
        logger.info(f"🔨 Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore', timeout=120)
        if result.returncode == 0:
            logger.info(f"✅ Installed {package_name}. Output:\n{result.stdout}")
            try:
                bot.send_message(message.from_user.id, f"✅ Installed `{package_name}`", parse_mode='Markdown')
            except Exception as e:
                logger.error(f"❌ Failed to send success message: {e}")
            return True
        else:
            error_msg = f"❌ Failed `{package_name}`\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Truncated)"
            try:
                bot.send_message(message.from_user.id, error_msg, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"❌ Failed to send error message: {e}")
            return False
    except subprocess.TimeoutExpired:
        error_msg = f"❌ Timeout `{package_name}`"
        logger.error(error_msg)
        try:
            bot.send_message(message.from_user.id, error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to send timeout message: {e}")
        return False
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        try:
            bot.send_message(message.from_user.id, error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to send error message: {e}")
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        try:
            bot.send_message(message.from_user.id, f"📦 Installing `{module_name}`...", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"❌ Failed to send install message: {e}")
            return False
            
        command = ['npm', 'install', module_name, '--timeout=60000']
        logger.info(f"🔨 Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore', timeout=120)
        if result.returncode == 0:
            logger.info(f"✅ Installed {module_name}. Output:\n{result.stdout}")
            try:
                bot.send_message(message.from_user.id, f"✅ Installed `{module_name}`", parse_mode='Markdown')
            except Exception as e:
                logger.error(f"❌ Failed to send success message: {e}")
            return True
        else:
            error_msg = f"❌ Failed `{module_name}`\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Truncated)"
            try:
                bot.send_message(message.from_user.id, error_msg, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"❌ Failed to send error message: {e}")
            return False
    except FileNotFoundError:
         error_msg = "❌ Node.js not found"
         logger.error(error_msg)
         try:
             bot.send_message(message.from_user.id, error_msg)
         except Exception as e:
             logger.error(f"❌ Failed to send node error message: {e}")
         return False
    except subprocess.TimeoutExpired:
        error_msg = f"❌ Timeout `{module_name}`"
        logger.error(error_msg)
        try:
            bot.send_message(message.from_user.id, error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to send timeout message: {e}")
        return False
    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        try:
            bot.send_message(message.from_user.id, error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to send error message: {e}")
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """run python script with automatic dependency installation"""
    max_attempts = 2 
    if attempt > max_attempts:
        try:
            bot.send_message(script_owner_id, f"❌ Failed to start `{file_name}`")
        except Exception as e:
            logger.error(f"❌ Failed to send error message: {e}")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run python script: {script_path}")

    try:
        if not os.path.exists(script_path):
            try:
                bot.send_message(script_owner_id, f"❌ File `{file_name}` not found")
            except Exception as e:
                logger.error(f"❌ Failed to send file not found message: {e}")
            return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"🔍 Running python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=10)
                return_code = check_proc.returncode
                logger.info(f"🔍 Python pre-check. rc: {return_code}. stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"📦 Detected missing python module: {module_name}")
                        try:
                            bot.send_message(script_owner_id, f"🔧 Installing `{module_name}`...")
                        except Exception as e:
                            logger.error(f"❌ Failed to send install message: {e}")
                        
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"✅ Install ok for {module_name}. Retrying run_script...")
                            try:
                                bot.send_message(script_owner_id, f"⚡ Restarting `{file_name}`...")
                            except Exception as e:
                                logger.error(f"❌ Failed to send restart message: {e}")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            try:
                                bot.send_message(script_owner_id, f"❌ Cannot run `{file_name}` - installation failed")
                            except Exception as e:
                                logger.error(f"❌ Failed to send error message: {e}")
                            return
            except subprocess.TimeoutExpired:
                logger.info("⏱️ Python pre-check timed out, imports likely ok.")
                if check_proc and check_proc.poll() is None: 
                    check_proc.kill()
                    check_proc.communicate()
            except Exception as e:
                 logger.error(f"❌ Error in python pre-check: {e}")
                 return

        logger.info(f"🚀 Starting python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: 
            log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"❌ Failed to open log file: {e}")
             try:
                 bot.send_message(script_owner_id, f"❌ Log file error for `{file_name}`")
             except Exception as e:
                 logger.error(f"❌ Failed to send log error message: {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], 
                cwd=user_folder, 
                stdout=log_file, 
                stderr=log_file,
                stdin=subprocess.PIPE, 
                startupinfo=startupinfo, 
                creationflags=creationflags,
                encoding='utf-8', 
                errors='ignore',
                bufsize=1
            )
            logger.info(f"✅ Started python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 
                'log_file': log_file, 
                'file_name': file_name,
                'chat_id': script_owner_id,  
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 
                'user_folder': user_folder, 
                'type': 'py', 
                'script_key': script_key
            }
            try:
                bot.send_message(script_owner_id, f"✅ `{file_name}` Running (PID: {process.pid})")
            except Exception as e:
                logger.error(f"❌ Failed to send success message: {e}")
            try:
                bot.delete_message(message_obj_for_reply.chat.id, message_obj_for_reply.message_id)
            except Exception as e:
                logger.warning(f"⚠️ Failed to delete message: {e}")  
        except Exception as e:
            if log_file and not log_file.closed: 
                log_file.close()
            error_msg = f"❌ Error starting `{file_name}`: {str(e)[:100]}"
            logger.error(error_msg, exc_info=True)
            try:
                bot.send_message(script_owner_id, error_msg)
            except Exception as e:
                logger.error(f"❌ Failed to send error message: {e}")
            if script_key in bot_scripts: 
                del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Error with `{file_name}`: {str(e)[:100]}"
        logger.error(error_msg, exc_info=True)
        try:
            bot.send_message(script_owner_id, error_msg)
        except Exception as e:
            logger.error(f"❌ Failed to send error message: {e}")

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """run js script with automatic dependency installation and robust environment handling"""
    max_attempts = 2
    if attempt > max_attempts:
        try:
            bot.send_message(script_owner_id, f"❌ Failed to start `{file_name}`\nCheck if your code has errors.")
        except Exception as e:
            logger.error(f"❌ Failed to send error message: {e}")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run js script: {script_path}")

    # --- ENV SETUP (အရေးကြီးဆုံးအပိုင်း) ---
    # Node.js က module တွေကို user folder ထဲမှာ ရှာတတ်အောင် လမ်းကြောင်းပေးခြင်း
    current_env = os.environ.copy()
    local_node_path = os.path.join(user_folder, 'node_modules')
    
    # ရှိပြီးသား NODE_PATH နဲ့ ပေါင်းထည့်မည်
    if 'NODE_PATH' in current_env:
        current_env['NODE_PATH'] = f"{local_node_path}:{current_env['NODE_PATH']}"
    else:
        current_env['NODE_PATH'] = local_node_path

    try:
        if not os.path.exists(script_path):
            try:
                bot.send_message(script_owner_id, f"❌ File `{file_name}` not found")
            except Exception as e:
                logger.error(f"❌ Failed to send file not found message: {e}")
            return

        if attempt == 1:
            # Check Command
            check_command = ['node', script_path]
            logger.info(f"🔍 Running js pre-check: {script_path}")
            check_proc = None
            try:
                # env=current_env ကို ထည့်ပေးရပါမယ်
                check_proc = subprocess.Popen(
                    check_command, 
                    cwd=user_folder, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    text=True, 
                    encoding='utf-8', 
                    errors='ignore',
                    env=current_env  # Module ရှာတွေ့အောင် Environment ထည့်ပေးခြင်း
                )
                
                # Timeout ကို 30 စက္ကန့်အထိ တိုးထားပါ (Render မှာ နှေးတတ်လို့ပါ)
                stdout, stderr = check_proc.communicate(timeout=30)
                return_code = check_proc.returncode
                
                logger.info(f"🔍 JS pre-check finished. RC: {return_code}")

                if return_code != 0 and stderr:
                    # Error ရှာခြင်း
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        # Local path တွေ (. or / နဲ့စတာတွေ) မဟုတ်မှ Install လုပ်မည်
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"📦 Detected missing node module: {module_name}")
                             try:
                                 bot.send_message(script_owner_id, f"📦 Installing `{module_name}`...")
                             except:
                                 pass
                             
                             # Install လုပ်ပြီးရင် ပြန် Run မည်
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"✅ Module {module_name} installed. Restarting...")
                                 try:
                                     bot.send_message(script_owner_id, f"⚡ Dependencies installed. Restarting `{file_name}`...")
                                 except:
                                     pass
                                 time.sleep(2)
                                 # Recursively call run_js_script again
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
            
            except subprocess.TimeoutExpired:
                logger.info("⏱️ JS pre-check timed out (Script is likely a long-running bot).")
                if check_proc and check_proc.poll() is None: 
                    check_proc.kill()
            except Exception as e:
                 logger.error(f"❌ Error in js pre-check: {e}")
                 # Error တက်လည်း ဆက် Run ကြည့်မည် (Sometimes check fails but run works)

        # --- REAL RUNNING PROCESS ---
        logger.info(f"🚀 Starting actual js process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None
        
        try: 
            log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"❌ Failed to open log file: {e}")
            return

        try:
            # Windows အတွက် Config (Linux/Render မှာ မလိုပေမယ့် ထည့်ထားခြင်း)
            startupinfo = None
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO()
                 startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            
            # တကယ် Run တဲ့နေရာမှာလည်း env=current_env ကို မဖြစ်မနေ ထည့်ရပါမယ်
            process = subprocess.Popen(
                ['node', script_path], 
                cwd=user_folder, 
                stdout=log_file, 
                stderr=log_file,
                stdin=subprocess.PIPE, 
                startupinfo=startupinfo, 
                encoding='utf-8', 
                errors='ignore',
                bufsize=1,
                env=current_env # Module Path ထည့်ပေးခြင်း
            )
            
            logger.info(f"✅ Started js process {process.pid} for {script_key}")
            
            # Save process info
            bot_scripts[script_key] = {
                'process': process, 
                'log_file': log_file, 
                'file_name': file_name,
                'chat_id': script_owner_id, 
                'script_owner_id': script_owner_id,
                'start_time': datetime.now(), 
                'user_folder': user_folder, 
                'type': 'js', 
                'script_key': script_key
            }
            
            try:
                bot.send_message(script_owner_id, f"✅ `{file_name}` is Running! (PID: {process.pid})")
            except Exception as e:
                logger.error(f"❌ Failed to send success message: {e}")
            
            try:
                bot.delete_message(message_obj_for_reply.chat.id, message_obj_for_reply.message_id)
            except:
                pass

        except Exception as e:
            if log_file and not log_file.closed: 
                log_file.close()
            error_msg = f"❌ Error starting `{file_name}`: {str(e)[:100]}"
            logger.error(error_msg, exc_info=True)
            try:
                bot.send_message(script_owner_id, error_msg)
            except:
                pass
            if script_key in bot_scripts: 
                del bot_scripts[script_key]

    except Exception as e:
        error_msg = f"❌ Critical Error with `{file_name}`: {str(e)[:100]}"
        logger.error(error_msg, exc_info=True)
        try:
            bot.send_message(script_owner_id, error_msg)
        except:
            pass

# --- Database  ---
DB_LOCK = threading.Lock()

def save_user(user_id, username, first_name, last_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('insert or replace into users (user_id, username, first_name, last_name) values (?, ?, ?, ?)',
                      (user_id, username, first_name, last_name))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Error saving user: {e}")
        finally:
            conn.close()

def save_user_file(user_id, file_name, file_type='unknown', file_path='', pending=False):
    """Save user file with chat ID and username"""
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('select username, first_name from users where user_id = ?', (user_id,))
            user_info = c.fetchone()
            username = user_info[0] if user_info else None
            first_name = user_info[1] if user_info else "Unknown"
            
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            c.execute('''insert into user_files 
                        (user_id, username, chat_id, file_name, file_type, file_path, 
                         original_filename, file_size, is_pending)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                     (user_id, username, user_id, file_name, file_type, file_path, 
                      file_name, file_size, 1 if pending else 0))
            
            conn.commit()
            
            if not pending:
                if user_id not in user_files:
                    user_files[user_id] = []
                user_files[user_id] = [(fn, ft, fp) for fn, ft, fp in user_files[user_id] if fn != file_name]
                user_files[user_id].append((file_name, file_type, file_path))
            
            logger.info(f"✅ File saved for user {user_id} (@{username}): {file_name} - Pending: {pending}")
            
        except Exception as e:
            logger.error(f"❌ Error saving file: {e}")
        finally:
            conn.close()

def approve_pending_file(user_id, file_name):
    """Approve a pending file and make it visible to user"""
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('update user_files set is_pending = 0 where user_id = ? and file_name = ?', 
                     (user_id, file_name))
            
            c.execute('select file_type, file_path from user_files where user_id = ? and file_name = ?', 
                     (user_id, file_name))
            result = c.fetchone()
            
            if result:
                file_type, file_path = result
                
                if user_id not in user_files:
                    user_files[user_id] = []
                user_files[user_id] = [(fn, ft, fp) for fn, ft, fp in user_files[user_id] if fn != file_name]
                user_files[user_id].append((file_name, file_type, file_path))
            
            conn.commit()
            logger.info(f"✅ File approved for user {user_id}: {file_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Error approving file: {e}")
            return False
        finally:
            conn.close()

def remove_user_file_db(user_id, file_name):
    """Remove user file from database and file system"""
    file_path = None
    
    if user_id in user_files:
        for fn, ft, fp in user_files[user_id]:
            if fn == file_name:
                file_path = fp
                break
    
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            if not file_path:
                c.execute('select file_path from user_files where user_id = ? and file_name = ?', (user_id, file_name))
                result = c.fetchone()
                if result:
                    file_path = result[0]
            
            c.execute('delete from user_files where user_id = ? and file_name = ?', (user_id, file_name))
            conn.commit()
            
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: 
                    del user_files[user_id]
            
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"✅ Deleted physical file: {file_path}")
                except Exception as e:
                    logger.error(f"❌ Error deleting physical file {file_path}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Error removing file from database: {e}")
        finally:
            conn.close()

def add_active_user(user_id):
    active_users.add(user_id)
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('insert or ignore into active_users (user_id) values (?)', (user_id,))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Error adding active user: {e}")
        finally:
            conn.close()

def save_subscription(user_id, expiry, file_limit=999):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('insert or replace into subscriptions (user_id, expiry, file_limit) values (?, ?, ?)', 
                     (user_id, expiry_str, file_limit))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry, 'file_limit': file_limit}
        except Exception as e:
            logger.error(f"❌ Error saving subscription: {e}")
        finally:
            conn.close()

def format_file_size(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f} {size_names[i]}"

def get_user_files_with_details(user_id):
    """Get all files for a user with complete details (non-pending only)"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''select file_id, file_name, file_type, file_path, 
                     original_filename, file_size, upload_date, is_active
                     from user_files 
                     where user_id = ? and is_pending = 0
                     order by upload_date desc''', (user_id,))
        files = c.fetchall()
        
        file_details = []
        for file in files:
            file_id, file_name, file_type, file_path, original_filename, file_size, upload_date, is_active = file
            
            size_str = format_file_size(file_size)
            
            is_running = is_bot_running(user_id, file_name)
            
            file_details.append({
                'file_id': file_id,
                'file_name': file_name,
                'file_type': file_type,
                'file_path': file_path,
                'original_filename': original_filename,
                'file_size': size_str,
                'upload_date': upload_date,
                'is_active': bool(is_active),
                'is_running': is_running
            })
        
        return file_details
    except Exception as e:
        logger.error(f"❌ Error getting user files: {e}")
        return []
    finally:
        conn.close()

def get_all_user_files_for_owner():
    """Get all files from all users - Owner only access"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''select u.user_id, u.username, u.first_name, 
                     f.file_name, f.file_type, f.file_size, f.upload_date, f.is_active, f.is_pending,
                     f.file_path
                     from user_files f
                     join users u on f.user_id = u.user_id
                     order by f.upload_date desc''')
        files = c.fetchall()
        
        files_by_user = {}
        for file in files:
            user_id, username, first_name, file_name, file_type, file_size, upload_date, is_active, is_pending, file_path = file
            
            if user_id not in files_by_user:
                files_by_user[user_id] = {
                    'username': username,
                    'first_name': first_name,
                    'files': []
                }
            
            files_by_user[user_id]['files'].append({
                'file_name': file_name,
                'file_type': file_type,
                'file_size': format_file_size(file_size),
                'upload_date': upload_date,
                'is_active': bool(is_active),
                'is_pending': bool(is_pending),
                'file_path': file_path
            })
        
        return files_by_user
    except Exception as e:
        logger.error(f"❌ Error getting all files: {e}")
        return {}
    finally:
        conn.close()

def get_user_by_key(key_value):
    """Get user who used a specific key"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''select u.user_id, u.username, u.first_name, u.key_used_date,
                     k.days_valid, k.file_limit, k.used_date
                     from users u
                     join subscription_keys k on u.key_used = k.key_value
                     where u.key_used = ?''', (key_value,))
        user = c.fetchone()
        
        if user:
            return {
                'user_id': user[0],
                'username': user[1],
                'first_name': user[2],
                'key_used_date': user[3],
                'days_valid': user[4],
                'file_limit': user[5],
                'key_activation_date': user[6]
            }
        return None
    except Exception as e:
        logger.error(f"❌ Error getting user by key: {e}")
        return None
    finally:
        conn.close()

def get_owner_files_summary():
    """Get summary of all files for owner dashboard"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('select count(*) from user_files')
        total_files = c.fetchone()[0]
        
        c.execute('select sum(file_size) from user_files')
        total_size = c.fetchone()[0] or 0
        
        c.execute('select file_type, count(*) from user_files group by file_type order by count(*) desc')
        files_by_type = c.fetchall()
        
        c.execute('''select u.user_id, u.username, u.first_name, count(f.file_id) as file_count
                     from users u
                     left join user_files f on u.user_id = f.user_id
                     group by u.user_id
                     order by file_count desc
                     limit 10''')
        top_users = c.fetchall()
        
        return {
            'total_files': total_files,
            'total_size': format_file_size(total_size),
            'files_by_type': files_by_type,
            'top_users': top_users
        }
    except Exception as e:
        logger.error(f"❌ Error getting owner summary: {e}")
        return None
    finally:
        conn.close()

# --- Ban User ---
def ban_user(user_id):
    """Ban a user from using the bot"""
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('select user_id from banned_users where user_id = ?', (user_id,))
            if c.fetchone():
                return False, "User already banned"
            
            c.execute('insert into banned_users (user_id, banned_by) values (?, ?)', 
                     (user_id, OWNER_ID))
            
            c.execute('delete from active_users where user_id = ?', (user_id,))
            
            cleanup_user_processes(user_id)
            
            c.execute('select file_path from user_files where user_id = ?', (user_id,))
            files = c.fetchall()
            for file_path, in files:
                if file_path and os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
            
            c.execute('delete from user_files where user_id = ?', (user_id,))
            
            c.execute('delete from subscriptions where user_id = ?', (user_id,))
            
            if user_id in active_users:
                active_users.remove(user_id)
            if user_id in user_files:
                del user_files[user_id]
            if user_id in user_subscriptions:
                del user_subscriptions[user_id]
            
            conn.commit()
            return True, "User banned successfully"
        except Exception as e:
            logger.error(f"❌ Error banning user: {e}")
            return False, f"Error: {str(e)}"
        finally:
            conn.close()

def unban_user(user_id):
    """Unban a user"""
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('delete from banned_users where user_id = ?', (user_id,))
            
            c.execute('insert or ignore into active_users (user_id) values (?)', (user_id,))
            active_users.add(user_id)
            
            conn.commit()
            return True, "User unbanned successfully"
        except Exception as e:
            logger.error(f"❌ Error unbanning user: {e}")
            return False, f"Error: {str(e)}"
        finally:
            conn.close()

def get_banned_users():
    """Get all banned users with details"""
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''select bu.user_id, bu.ban_date, bu.reason, 
                     u.username, u.first_name, u.last_name
                     from banned_users bu
                     left join users u on bu.user_id = u.user_id
                     order by bu.ban_date desc''')
        return c.fetchall()
    finally:
        conn.close()

# --- Menu Creation ---
def create_main_menu_keyboard(user_id):
    """create modern main menu keyboard"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    buttons = [
        '📤 Upload File',
        '📂 My Files', 
        '🔑 Redeem Key',
        '💎 Upgrade',
        '👤 Profile',
        '📊 Statistics'
    ]
    
    if user_id in admin_ids:
        buttons.append('⚙️ Admin Dashboard')
    
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.row(buttons[i], buttons[i+1])
        else:
            markup.row(buttons[i])
    
    return markup

def create_start_hosting_keyboard():
    """create start hosting button"""
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('🚀 Deploy Now', callback_data='start_hosting'))
    return markup

def create_manage_files_keyboard(user_id):
    """create modern files management keyboard"""
    user_files_list = user_files.get(user_id, [])
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if not user_files_list:
        markup.add(types.InlineKeyboardButton("📭 No Files", callback_data='no_files'))
    else:
        for file_name, file_type, file_path in user_files_list:
            is_running = is_bot_running(user_id, file_name)
            status_emoji = "🟢" if is_running else "🔴"
            button_text = f"{status_emoji} {file_name}"
            markup.add(types.InlineKeyboardButton(button_text, callback_data=f'file_{user_id}_{file_name}'))
    
    markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data='back_to_main'))
    return markup

def create_file_management_buttons(user_id, file_name, is_running=True):
    markup = types.InlineKeyboardMarkup(row_width=2)
    if is_running:
        markup.row(
            types.InlineKeyboardButton("⏸️ Stop", callback_data=f'stop_{user_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{user_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("▶️ Start", callback_data=f'start_{user_id}_{file_name}'),
        )
    markup.row(
        types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{user_id}_{file_name}'),
        types.InlineKeyboardButton("📋 Logs", callback_data=f'logs_{user_id}_{file_name}')
    )
    markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data='manage_files'))
    return markup

def create_admin_panel_keyboard(user_id=None):
    """create modern admin panel with owner-only options"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    buttons = [
        '📊 Users Stats',
        '👥 Users',
        '💎 Premium Users',
        '🔑 Generate', 
        '🔍 Key-User',
        '🗑️ Revoke',
        '🔢 Keys',
        '⬅️ Back'
    ]
    
    if user_id == OWNER_ID:
        owner_buttons = [
            '➕ Add Admin',
            '➖ Remove Admin',
            '🚫 Ban User',
            '✅ Unban User',
            '📋 Banned',
            '📢 Broadcast',
            '📈 Limits',
            '⚙️ Settings',
            '📁 All Files',
            '🛡️ Security Logs',
            '🛑 Force Stop'
        ]
        buttons = owner_buttons + buttons
    
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.row(buttons[i], buttons[i+1])
        else:
            markup.row(buttons[i])
    
    return markup

@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message):
    user_id = message.from_user.id
    
    if message.chat.type in ['group', 'supergroup']:
        return

    if is_user_banned(user_id):
        bot.send_message(message.chat.id, 
                        f"""
🚫 *YOU ARE BANNED*
⚠️ Your access has been revoked.

👑 **Contact Support:** {YOUR_USERNAME}
                        """,
                        parse_mode='Markdown')
        return

    if bot_locked and user_id not in admin_ids:
        bot.send_message(message.chat.id, 
                        f"""
🔒 *MAINTENANCE MODE*
⚠️ Temporarily unavailable.
Please try again later.

👑 **Contact Support:** {YOUR_USERNAME}
                        """,
                        parse_mode='Markdown')
        return

    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.send_message(message.chat.id, force_message, reply_markup=force_markup, parse_mode='Markdown')
        return
    
    add_active_user(user_id)
    save_user(user_id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    
    user_file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    if user_file_limit == float('inf'):
        limit_display = 'Unlimited'
    else:
        limit_display = user_file_limit
    
    welcome_text = f"""
☁️ **KELVIN VPS HOST** ☁️

✨ *Welcome back, {message.from_user.first_name}!*

🚀 **Advanced Cloud Execution Platform**
-----------------------------------------
├─📦 Supports 30+ Languages
├─⚡ Auto Dependency Install
└─🔧 Real-Time Monitoring


ACCOUNT STATUS      
-----------------
├─ Plan: {get_user_status(user_id)}
└─ Storage: {current_files}/{limit_display}


💎 **Upgrade to Premium:**
----------------------------
├─ 7 Days: 2000Ks / $0.50 (5 Files)
├─ 30 Days: 8000Ks / $2.00 (15 Files)  
├─ 90 Days: 23000Ks / $5.50 (Unlimited)
├─ 1 Year: 80000Ks / $20.00 (Unlimited)
└─ Lifetime: 200000Ks / $50.00 (Unlimited)

Select an option below to begin.
    """
    
    markup = create_main_menu_keyboard(user_id)
    bot.send_message(message.chat.id, welcome_text, reply_markup=markup, parse_mode='Markdown')

# --- Security Scan ---
@bot.message_handler(content_types=['document'])
def handle_document_secure(message):
    user_id = message.from_user.id
    username = message.from_user.username

    if message.chat.type in ['group', 'supergroup']:
        return  

    if is_user_banned(user_id):
        bot.reply_to(message,
                    f"""
🚫 *YOU ARE BANNED*
⚠️ Your access has been revoked.

👑 **Contact Support:** {YOUR_USERNAME}
                    """,
                    parse_mode='Markdown')
        return

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, 
                    f"""
🔒 *MAINTENANCE MODE*
⚠️ Temporarily unavailable.
Please try again later.

👑 **Contact Support:** {YOUR_USERNAME}
                    """,
                    parse_mode='Markdown')
        return
    
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.send_message(message.chat.id, force_message, reply_markup=force_markup, parse_mode='Markdown')
        return
    
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    if current_files >= file_limit:
        if is_premium_user(user_id):
            subscription_info = user_subscriptions.get(user_id, {})
            premium_limit = subscription_info.get('file_limit', PREMIUM_USER_LIMIT)
            bot.reply_to(message, f"❌ Storage limit reached ({premium_limit} files).\n💎 Upgrade for more space.")
        else:
            bot.reply_to(message, f"❌ Storage limit reached ({FREE_USER_LIMIT} files).\n💎 Upgrade to Premium for more space.")
        return
    
    doc = message.document
    file_name = doc.file_name
    file_ext = os.path.splitext(file_name)[1].lower()
    
    if file_ext not in SUPPORTED_EXTENSIONS:
        supported_list = ", ".join([f"`{ext}`" for ext in sorted(SUPPORTED_EXTENSIONS.keys())])
        bot.reply_to(message, f"❌ Unsupported File Type\n\nSupported: {supported_list}", parse_mode='Markdown')
        return
    
    try:
        file_info = bot.get_file(doc.file_id)
        downloaded_file = bot.download_file(file_info.file_path)
        
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        temp_path = file_path + '.scan'
        with open(temp_path, 'wb') as new_file:
            new_file.write(downloaded_file)
        
        threats = scan_file_for_threats(temp_path, user_id, username, file_name)
        
        if threats:
            report = generate_threat_report(threats, user_id, username, file_name, temp_path)
            
            if report and report['critical_risk']:
                os.remove(temp_path)
                security_scans['blocked_files'] += 1
                security_scans['high_risk_files'] += 1
                
                bot.reply_to(message, 
                    f"""🚨 **SECURITY ALERT** 🚨

❌ File `{file_name}` BLOCKED
⛔ Critical threat detected.
🔒 File deleted for security.

📞 Contact admin immediately.
                    """,
                    parse_mode='Markdown'
                )
                
                send_threat_alert_to_owner(report)
                return
            
            elif report and report['high_risk']:
                os.remove(temp_path)
                security_scans['blocked_files'] += 1
                security_scans['high_risk_files'] += 1
                
                bot.reply_to(message, 
                    f"""🚨 **SECURITY ALERT** 🚨

❌ File `{file_name}` BLOCKED
⚠️ High risk threat detected.
🔒 File deleted for security.

📞 Contact admin if this is a mistake.
                    """,
                    parse_mode='Markdown'
                )
                
                send_threat_alert_to_owner(report)
                return
            
            elif report:
                os.rename(temp_path, file_path)
                
                file_type = SUPPORTED_EXTENSIONS.get(file_ext, 'Unknown')
                save_user_file(user_id, file_name, file_type, file_path, pending=True)
                
                try:
                    bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
                    user_mention = f"[{message.from_user.first_name}](tg://user?id={user_id})" if message.from_user.first_name else f"User {user_id}"
                    bot.send_message(OWNER_ID, 
                                   f"""
📤 File Uploaded (Pending Review)
👤User: {user_mention}
🤖ID: `{user_id}`
📄File Name: `{file_name}`
📦File Type:{file_type}
⚠️ Status: **PENDING OWNER REVIEW**
                                   """,
                                   parse_mode='Markdown')
                except Exception as e:
                    logger.error(f"❌ Failed to notify owner: {e}")

                review_pending_msg = bot.reply_to(message,
                    f"""⚠️ **UPLOAD PENDING REVIEW** ⚡

`{file_name}` uploaded successfully.
⚠️ Suspicious patterns detected.
👑 Admin has been notified.
🛡️ File will be reviewed shortly.
                    """,
                    parse_mode='Markdown'
                )

                send_threat_alert_to_owner(report)
                
        else:
            os.rename(temp_path, file_path)
            
            file_type = SUPPORTED_EXTENSIONS.get(file_ext, 'Unknown')
            save_user_file(user_id, file_name, file_type, file_path, pending=False)
            
            try:
                bot.forward_message(OWNER_ID, message.chat.id, message.message_id)
                user_mention = f"[{message.from_user.first_name}](tg://user?id={user_id})" if message.from_user.first_name else f"User {user_id}"
                bot.send_message(OWNER_ID, 
                               f"""
📤 New File
👤User: {user_mention}
🤖ID: `{user_id}`
📄File Name: `{file_name}`
📦File Type:{file_type}
                               """,
                               parse_mode='Markdown')
            except Exception as e:
                logger.error(f"❌ Failed to notify owner: {e}")
            
            limit_display = str(file_limit) if file_limit != float('inf') else "Unlimited"
            
            success_text = f"""
✅ **UPLOAD SUCCESS**

File: `{file_name}`
Type: {file_type}


    STORAGE USAGE 
---------------------
Used: {current_files + 1}/{limit_display}


Tap Deploy to start.
            """
            
            markup = create_start_hosting_keyboard()
            bot.reply_to(message, success_text, reply_markup=markup, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"❌ Error uploading file: {e}")
        bot.reply_to(message, f"❌ Error: {str(e)}")

@bot.message_handler(func=lambda message: True)
def handle_text_messages(message):
    user_id = message.from_user.id

    if message.chat.type in ['group', 'supergroup']:
        return  

    if not (user_id in admin_ids or user_id == OWNER_ID) and is_user_banned(user_id):
        bot.send_message(message.chat.id, 
                        f"""
🚫 *YOU ARE BANNED*
⚠️ Your access has been revoked.

👑 **Contact Support:** {YOUR_USERNAME}
                        """,
                        parse_mode='Markdown')
        return

    if bot_locked and user_id not in admin_ids:
        bot.send_message(message.chat.id, 
                        f"""
🔧 *MAINTENANCE MODE*
⚠️ Temporarily unavailable.
Please try again later.

👑 **Contact Support:** {YOUR_USERNAME}
                        """,
                        parse_mode='Markdown')
        return
        
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.send_message(message.chat.id, force_message, reply_markup=force_markup, parse_mode='Markdown')
        return
    
    text = message.text

    if text == '📁 All Files' and user_id == OWNER_ID:
        handle_admin_files_text(message)
    elif text == '🛡️ Security Logs' and user_id == OWNER_ID:
        handle_security_logs_text(message)
    elif text == '🔍 Key-User' and user_id in admin_ids:
        handle_key_user_info_text(message)
    elif text == '📊 Users Stats' and user_id in admin_ids:
        handle_bot_statistics_text(message)
    elif text == '👥 Users' and user_id in admin_ids:
        handle_all_users_text(message)
    elif text == '💎 Premium Users' and user_id in admin_ids:
        handle_premium_users_text(message)
    elif text == '📢 Broadcast' and user_id in admin_ids:
        handle_broadcast_text(message)
    elif text == '🔑 Generate' and user_id in admin_ids:
        handle_generate_key_text(message)
    elif text == '🗑️ Revoke' and user_id in admin_ids:
        handle_delete_key_text(message)
    elif text == '🔢 Keys' and user_id in admin_ids:
        handle_total_keys_text(message)
    elif text == '📈 Limits' and user_id in admin_ids:
        handle_file_limit_text(message)
    elif text == '⚙️ Settings' and user_id in admin_ids:
        handle_bot_settings_text(message)
    elif text == '➕ Add Admin' and user_id == OWNER_ID:
        handle_add_admin_text(message)
    elif text == '➖ Remove Admin' and user_id == OWNER_ID:
        handle_remove_admin_text(message)
    elif text == '🚫 Ban User' and user_id == OWNER_ID:
        handle_ban_user_text(message)
    elif text == '✅ Unban User' and user_id == OWNER_ID:
        handle_unban_user_text(message)
    elif text == '📋 Banned' and user_id == OWNER_ID:
        handle_view_banned_users_text(message)
    elif text == '🛑 Force Stop' and user_id in admin_ids:
        handle_force_stop_user_text(message)
    elif text == '⬅️ Back':
        handle_back_to_main_text(message)
    elif text == '📤 Upload File':
        handle_upload_file_text(message)
    elif text == '📂 My Files':
        handle_manage_files_text(message)
    elif text == '🔑 Redeem Key':
        handle_redeem_key_text(message)
    elif text == '💎 Upgrade':
        handle_buy_subscription_text(message)
    elif text == '👤 Profile':
        handle_my_info_text(message)
    elif text == '📊 Statistics':
        handle_status_text(message)
    elif text == '⚙️ Admin Dashboard' and user_id in admin_ids:
        handle_admin_panel_text(message)
    else:
        bot.send_message(message.chat.id, "❌ Invalid Command")

def handle_security_logs_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute('''select username, file_name, threat_count, risk_level, 
                     action_taken, log_date from security_logs 
                     order by log_date desc limit 20''')
        logs = c.fetchall()
        
        if not logs:
            bot.send_message(message.chat.id, "📭 No Security Logs")
            return
        
        html_text = "<b>🛡️ SECURITY LOGS</b>\n\n"
        
        for log in logs:
            username, file_name, threat_count, risk_level, action_taken, log_date = log
            
            risk_emoji = "🔴" if risk_level == 'critical' else "🟠" if risk_level == 'high' else "🟡"
            
            html_text += f"""
<b>{risk_emoji} {risk_level.upper()}</b>
├─ <b>User:</b> @{username}
├─ <b>File:</b> <code>{file_name}</code>
├─ <b>Threats:</b> {threat_count}
├─ <b>Action:</b> {action_taken}
└─ <b>Time:</b> {log_date[:19]}
"""
            html_text += "─" * 35 + "\n"
        
        c.execute('select count(*) from security_logs')
        total_logs = c.fetchone()[0]
        
        html_text += f"""
<b>📊 SUMMARY:</b>
├─ <b>Total Logs:</b> {total_logs}
├─ <b>Total Scans:</b> {security_scans['total_scans']}
├─ <b>Threats Found:</b> {security_scans['threats_found']}
└─ <b>Blocked Files:</b> {security_scans['blocked_files']}
        """
        
        bot.send_message(message.chat.id, html_text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"❌ Error getting security logs: {e}")
        bot.send_message(message.chat.id, f"❌ Error: {str(e)}")
    finally:
        conn.close()

def handle_add_admin_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    msg = bot.send_message(message.chat.id, "🆔 Enter Admin ID:")
    bot.register_next_step_handler(msg, process_add_admin)

def process_add_admin(message):
    try:
        admin_id = int(message.text.strip())
        
        if admin_id == OWNER_ID:
            bot.send_message(message.chat.id, "❌ Can't add Owner")
            return
        
        if add_admin_to_db(admin_id):
            admin_ids.add(admin_id)
            
            try:
                user_info = bot.get_chat(admin_id)
                username = f"@{user_info.username}" if user_info.username else "N/A"
                name = user_info.first_name
                
                bot.send_message(message.chat.id, 
                                f"""
✅ **ADMIN ADDED**

👤 {name}
🆔 {admin_id}
👥 {username}
                """, 
                                parse_mode='Markdown')
                
                bot.send_message(admin_id, 
                                f"""
🛡️ **YOU HAVE BEEN PROMOTED**

👑 By: {message.from_user.first_name}
🔑 Access: Full Admin Dashboard

Use /start to see your new menu.
                """, 
                                parse_mode='Markdown')
            except Exception as e:
                bot.send_message(message.chat.id, f"✅ Admin added (id: {admin_id})")
                logger.error(f"❌ Failed to get user info: {e}")
        else:
            bot.send_message(message.chat.id, "❌ Failed to add admin")
            
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid ID")

def handle_remove_admin_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    admins = get_all_admins()
    if not admins:
        bot.send_message(message.chat.id, "📭 No admins")
        return
    
    admin_list = "🛡️ **CURRENT ADMINS:**\n\n"
    for admin_id in admins:
        if admin_id != OWNER_ID:
            try:
                user_info = bot.get_chat(admin_id)
                username = f"@{user_info.username}" if user_info.username else "N/A"
                admin_list += f"👤 {user_info.first_name} - `{admin_id}` {username}\n"
            except:
                admin_list += f"👤 Unknown - `{admin_id}`\n"
    
    admin_list += "\n🆔 Enter admin ID to remove:"
    msg = bot.send_message(message.chat.id, admin_list, parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_remove_admin)

def process_remove_admin(message):
    try:
        admin_id = int(message.text.strip())
        
        if admin_id == OWNER_ID:
            bot.send_message(message.chat.id, "❌ Can't remove Owner")
            return
        
        if admin_id not in admin_ids:
            bot.send_message(message.chat.id, "❌ Not an admin")
            return
        
        if remove_admin_from_db(admin_id):
            admin_ids.discard(admin_id)
            
            try:
                user_info = bot.get_chat(admin_id)
                username = f"@{user_info.username}" if user_info.username else "N/A"
                name = user_info.first_name
                
                bot.send_message(message.chat.id, 
                                f"""
❌ **ADMIN REMOVED**

👤 {name}
🆔 {admin_id}
👥 {username}
                """, 
                                parse_mode='Markdown')
                
                bot.send_message(admin_id, 
                                f"""
⚠️ **YOU HAVE BEEN REMOVED**

👑 By: {message.from_user.first_name}
🔑 Access: Revoked
                """, 
                                parse_mode='Markdown')
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Admin removed (id: {admin_id})")
                logger.error(f"❌ Failed to get user info: {e}")
        else:
            bot.send_message(message.chat.id, "❌ Failed to remove admin")
            
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid ID")

def handle_bot_settings_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if message.from_user.id == OWNER_ID:
        force_status = "🟢 Enabled" if force_join_enabled else "🔴 Disabled"
        markup.add(types.InlineKeyboardButton(f"🔐 Force Join: {force_status}", callback_data='toggle_force_join'))
    
    if message.from_user.id == OWNER_ID:
        lock_status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        markup.add(types.InlineKeyboardButton(f"🔒 Bot Status: {lock_status}", callback_data='toggle_bot_lock'))
    
    markup.add(types.InlineKeyboardButton(f"🗃 File Limit: {FREE_USER_LIMIT}", callback_data='change_file_limit'))
    
    if message.from_user.id == OWNER_ID:
        markup.add(types.InlineKeyboardButton("🛡️ Security Stats", callback_data='security_stats'))
    
    markup.add(types.InlineKeyboardButton("📢 Broadcast", callback_data='broadcast_settings'))
    markup.add(types.InlineKeyboardButton("ℹ️ System Info", callback_data='system_info'))
    
    settings_text = f"""
⚙️ **BOT SETTINGS**

👤 **Admin:** {message.from_user.first_name}
🆔 **ID:** `{message.from_user.id}`
---------------------------------------------

🔐 **Force Join:** {'Enabled' if force_join_enabled else 'Disabled'}
🔒 **Bot Lock:** {'Locked' if bot_locked else 'Unlocked'}
🗃 **File Limit:** {FREE_USER_LIMIT}
🛡️ **Scans:** {security_scans['total_scans']}
---------------------------------------------

    """
    
    bot.send_message(message.chat.id, settings_text, reply_markup=markup, parse_mode='Markdown')

@bot.callback_query_handler(func=lambda call: call.data == 'toggle_force_join')
def callback_toggle_force_join(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    try:
        new_status = not force_join_enabled
        update_force_join_status(new_status)
        
        if new_status:
            response_text = "✅ Force Join has been ENABLED"
            bot.answer_callback_query(call.id, "✅ Enabled", show_alert=False)
        else:
            response_text = "❌ Force Join has been DISABLED"
            bot.answer_callback_query(call.id, "❌ Disabled", show_alert=False)
        
        force_status = "🟢 Enabled" if new_status else "🔴 Disabled"
        lock_status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton(f"🔐 Force Join: {force_status}", callback_data='toggle_force_join'))
        markup.add(types.InlineKeyboardButton(f"🔒 Bot Status: {lock_status}", callback_data='toggle_bot_lock'))
        markup.add(types.InlineKeyboardButton(f"📊 File Limit: {FREE_USER_LIMIT}", callback_data='change_file_limit'))
        
        if call.from_user.id == OWNER_ID:
            markup.add(types.InlineKeyboardButton("🛡️ Security Stats", callback_data='security_stats'))
        
        markup.add(types.InlineKeyboardButton("📢 Broadcast", callback_data='broadcast_settings'))
        markup.add(types.InlineKeyboardButton("ℹ️ System Info", callback_data='system_info'))
        
        settings_text = f"""
⚙️ **BOT SETTINGS**

👤 **Admin:** {call.from_user.first_name}
🆔 **ID:** `{call.from_user.id}`
---------------------------------------------

🔐 **Force Join:** {'Enabled' if new_status else 'Disabled'}
🔧 **Bot Lock:** {'Locked' if bot_locked else 'Unlocked'}
🗃 **File Limit:** {FREE_USER_LIMIT}
🛡️ **Scans:** {security_scans['total_scans']}
---------------------------------------------

        """
        
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=settings_text,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        
        bot.send_message(call.message.chat.id, response_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error toggling force join: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'toggle_bot_lock')
def callback_toggle_bot_lock(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    try:
        global bot_locked
        bot_locked = not bot_locked
        
        if bot_locked:
            response_text = "🔒 Bot has been LOCKED"
            bot.answer_callback_query(call.id, "🔒 Locked", show_alert=False)
        else:
            response_text = "🔓 Bot has been UNLOCKED"
            bot.answer_callback_query(call.id, "🔓 Unlocked", show_alert=False)
        
        force_status = "🟢 Enabled" if force_join_enabled else "🔴 Disabled"
        lock_status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton(f"🔐 Force Join: {force_status}", callback_data='toggle_force_join'))
        markup.add(types.InlineKeyboardButton(f"🔒 Bot Status: {lock_status}", callback_data='toggle_bot_lock'))
        markup.add(types.InlineKeyboardButton(f"📊 File Limit: {FREE_USER_LIMIT}", callback_data='change_file_limit'))
        
        if call.from_user.id == OWNER_ID:
            markup.add(types.InlineKeyboardButton("🛡️ Security Stats", callback_data='security_stats'))
        
        markup.add(types.InlineKeyboardButton("📢 Broadcast", callback_data='broadcast_settings'))
        markup.add(types.InlineKeyboardButton("ℹ️ System Info", callback_data='system_info'))
        
        settings_text = f"""
⚙️ **BOT SETTINGS**

👤 **Admin:** {call.from_user.first_name}
🆔 **ID:** `{call.from_user.id}`
---------------------------------------------

🔐 **Force Join:** {'Enabled' if force_join_enabled else 'Disabled'}
🔧 **Bot Lock:** {'Locked' if bot_locked else 'Unlocked'}
🗃 **File Limit:** {FREE_USER_LIMIT}
🛡️ **Scans:** {security_scans['total_scans']}
---------------------------------------------

        """
        
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=settings_text,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        
        bot.send_message(call.message.chat.id, response_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error toggling bot lock: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'change_file_limit')
def callback_change_file_limit(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")

    msg = bot.send_message(call.message.chat.id, "📊 Enter new file limit for free users:")
    bot.register_next_step_handler(msg, process_file_limit_change)

def process_file_limit_change(message):
    try:
        new_limit = int(message.text.strip())
        if new_limit < 0:
            bot.send_message(message.chat.id, "❌ Limit must be positive")
            return
        
        update_file_limit(new_limit)
        bot.send_message(message.chat.id, f"✅ File limit updated to {new_limit}")
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid number")

@bot.callback_query_handler(func=lambda call: call.data == 'security_stats')
def callback_security_stats(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        
        c.execute('select count(*) from security_logs')
        total_logs = c.fetchone()[0]
        
        c.execute("select count(*) from security_logs where risk_level = 'critical'")
        critical_logs = c.fetchone()[0]
        
        c.execute("select count(*) from security_logs where risk_level = 'high'")
        high_logs = c.fetchone()[0]
        
        c.execute("select count(distinct user_id) from security_logs")
        affected_users = c.fetchone()[0]
        
        conn.close()
        
        stats_text = f"""
🛡️ **SECURITY STATISTICS**

📊 **SCANNING:**
├─ Total Scans: {security_scans['total_scans']}
├─ Threats Found: {security_scans['threats_found']}
├─ High Risk Files: {security_scans['high_risk_files']}
└─ Blocked Files: {security_scans['blocked_files']}

📈 **LOGGED EVENTS:**
├─ Total Logs: {total_logs}
├─ Critical Events: {critical_logs}
├─ High Risk Events: {high_logs}
└─ Affected Users: {affected_users}

⚡ **SYSTEM STATUS:**
├─ Security Scanner: 🟢 ACTIVE
├─ Real-time Monitoring: 🟢 ENABLED
└─ Owner Alerts: 🟢 ENABLED
        """
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📋 View Logs", callback_data='view_security_logs'))
        markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data='back_to_admin_settings'))
        
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=stats_text,
            reply_markup=markup,
            parse_mode='Markdown'
        )
        
    except Exception as e:
        logger.error(f"Error getting security stats: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'view_security_logs')
def callback_view_security_logs(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    handle_security_logs_text(call.message)

@bot.callback_query_handler(func=lambda call: call.data == 'back_to_admin_settings')
def callback_back_to_admin_settings(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    handle_bot_settings_text(call.message)

@bot.callback_query_handler(func=lambda call: call.data == 'broadcast_settings')
def callback_broadcast_settings(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
    
    msg = bot.send_message(call.message.chat.id, "📢 Enter message to broadcast:")
    bot.register_next_step_handler(msg, process_broadcast)

def process_broadcast(message):
    try:
        broadcast_text = message.text
        success_count = 0
        fail_count = 0
        
        for user_id in active_users:
            try:
                bot.send_message(user_id, broadcast_text)
                success_count += 1
                time.sleep(0.1)  
            except:
                fail_count += 1
        
        bot.send_message(
            message.chat.id,
            f"📢 **Broadcast Complete**\n\n✅ Success: {success_count}\n❌ Failed: {fail_count}",
            parse_mode='Markdown'
        )
        
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Error: {str(e)}")

@bot.callback_query_handler(func=lambda call: call.data == 'system_info')
def callback_system_info(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")

    try:
        stats = get_bot_statistics()
        
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        info_text = f"""
ℹ️ **System Information**

📊 **Bot Statistics:**
• Total Users: {stats['total_users']}
• Total Files: {stats['total_files']}
• Active Files: {stats['active_files']}
• Premium Users: {stats['premium_users']}
• Security Alerts: {stats['security_alerts']}
• Banned Users: {stats['banned_users']}

💻 **System Resources:**
• CPU Usage: {cpu_percent}%
• Memory: {memory.percent}% ({memory.used/1024/1024/1024:.1f}GB/{memory.total/1024/1024/1024:.1f}GB)
• Disk: {disk.percent}% ({disk.used/1024/1024/1024:.1f}GB/{disk.total/1024/1024/1024:.1f}GB)

⚙️ **Bot Settings:**
• Force Join: {'Enabled' if force_join_enabled else 'Disabled'}
• Bot Lock: {'Locked' if bot_locked else 'Unlocked'}
• Free User Limit: {FREE_USER_LIMIT}
        """
        
        bot.send_message(call.message.chat.id, info_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == 'back_to_admin')
def callback_back_to_admin(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    handle_admin_panel_text(call.message)

def handle_admin_panel_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    markup = create_admin_panel_keyboard(message.from_user.id)
    
    if message.from_user.id == OWNER_ID:
        role_text = "👑 Owner"
        features = "• 📁 View all user files\n• 👑 Full system access\n• 🛡️ Security monitoring"
    else:
        role_text = "🛡️ Admin"
        features = "• 👥 User management\n• 🔑 Key management"
    
    admin_text = f"""
🛡️ **ADMIN DASHBOARD**

👤 **User:** {message.from_user.first_name}
🆔 **ID:** `{message.from_user.id}`
📋 **Role:** {role_text}

📊 **Statistics:**
• Total Users: {len(active_users)}
• Total Files: {sum(len(files) for files in user_files.values())}
• Premium Users: {sum(1 for user_id in active_users if is_premium_user(user_id))}
• Security Scans: {security_scans['total_scans']}

⚙️ **Your Features:**
{features}
    """
    
    bot.send_message(message.chat.id, admin_text, reply_markup=markup, parse_mode='Markdown')

def handle_admin_files_text(message):
    user_id = message.from_user.id
    
    if user_id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    files_data = get_all_user_files_for_owner()
    
    if not files_data:
        bot.send_message(message.chat.id, "📭 No files found")
        return
    
    files_text = "👑 **OWNER VIEW - ALL USER FILES:**\n\n"
    
    for user_id, user_data in list(files_data.items())[:20]: 
        username = f"@{user_data['username']}" if user_data['username'] else "No Username"
        files_text += f"👤 **{user_data['first_name']}** ({username}) - `{user_id}`\n"
        
        for file in user_data['files'][:5]: 
            status = "🟡 Pending" if file['is_pending'] else "🟢" if file['is_active'] else "🔴"
            files_text += f"  {status} `{file['file_name']}` ({file['file_size']}) - {file['upload_date'][:10]}\n"
            
            files_text += f"  📍 `{file['file_path'][-50:]}`\n"
        
        files_text += "\n"
    
    if len(files_data) > 20:
        files_text += f"\n... {len(files_data) - 20} more users"
    
    total_users = len(files_data)
    total_files = sum(len(user_data['files']) for user_data in files_data.values())
    
    files_text += f"\n📊 **SUMMARY:** {total_files} files from {total_users} users"
    
    bot.send_message(message.chat.id, files_text, parse_mode='Markdown')

def handle_upload_file_text(message):
    user_id = message.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    if current_files >= file_limit and not is_premium_user(user_id):
        bot.send_message(message.chat.id, f"❌ Storage limit reached ({FREE_USER_LIMIT} files)\n💎 Upgrade for more space.")
        return
    
    supported_files = ", ".join([ext for ext in SUPPORTED_EXTENSIONS.keys()])
    bot.send_message(message.chat.id, 
                    f"""
📤 **UPLOAD FILE**

Supported: `{supported_files}`

Upload your script or code file now.
Auto-deploy is available.
                    """,
                    parse_mode='Markdown')

def handle_manage_files_text(message):
    user_id = message.from_user.id
    user_files_list = user_files.get(user_id, [])
    
    if not user_files_list:
        bot.send_message(message.chat.id, "📭 No Files")
        return
    
    files_text = f"📂 **MY FILES:**\n\n"
    
    for file_name, file_type, file_path in user_files_list:
        is_running = is_bot_running(user_id, file_name)
        status = "🟢 Running" if is_running else "🔴 Stopped"
        files_text += f"• `{file_name}` - {status}\n"
    
    files_text += "\nTap file to manage"
    
    markup = create_manage_files_keyboard(user_id)
    bot.send_message(message.chat.id, files_text, reply_markup=markup, parse_mode='Markdown')

def handle_redeem_key_text(message):
    msg = bot.send_message(message.chat.id, "🔑 Enter Key (KELVIN-XXXX-XXXX):")
    bot.register_next_step_handler(msg, process_redeem_key)

def handle_buy_subscription_text(message):
    html_text = """
<b>💎 UPGRADE PREMIUM PLANS</b>

<b>♣️ WEEKLY PLANS</b>
──────────────
│ <b>Price:</b> $0.50 / 2000 Ks
│ <b>Files:</b> 5 Files
└─ <b>Support:</b> Basic

<b>♦️ MONTHLY PLANS(popular)</b>
──────────────
│ <b>Price:</b> $2.00 / 8000 Ks
│ <b>Files:</b> 15 Files
└─ <b>Support:</b> Standard

<b>♥️ 3 MONTHS</b>
──────────────
│ <b>Price:</b> $5.50 / 23000 Ks
│ <b>Files:</b> Unlimited
└─ <b>Support:</b> Priority

<b>♠️ 1 YEAR</b>
──────────────
│ <b>Price:</b> $20.00 / 80000 Ks
│ <b>Files:</b> Unlimited & Bot Admin
└─ <b>Support:</b> Priority+

<b>⚡ LIFETIME</b>
───────────────
│ <b>Price:</b> $50.00 / 200000 Ks
│ <b>Files:</b> Unlimited & Bot Admin & Bot Source
└─ <b>Support:</b> 24/7 VIP

<b>💳 Payment Methods:</b>
• Binance
• Bybit
• KPAY
• WAVE

<b>📲 Contact Support:</b> """ + YOUR_USERNAME + """
    """
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("💳 Contact Support", url=f"https://t.me/{YOUR_USERNAME[1:]}"))
    markup.add(types.InlineKeyboardButton("🔑 Redeem Key", callback_data='redeem_key'))
    
    bot.send_message(message.chat.id, html_text, reply_markup=markup, parse_mode='HTML')

def handle_bot_statistics_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    stats = get_bot_statistics()
    
    html_text = f"""
<b>📊 SYSTEM STATS</b>

👥 <b>Users:</b> <code>{stats['total_users']}</code>
💎 <b>Premium Users:</b> <code>{stats['premium_users']}</code>
📁 <b>Total Files:</b> <code>{stats['total_files']}</code>
🟢 <b>Active Files:</b> <code>{stats['active_files']}</code>

<b>🛡️ SECURITY:</b>
├─ 🔍 <b>Scans:</b> <code>{security_scans['total_scans']}</code>
├─ ⚠️ <b>Threats:</b> <code>{security_scans['threats_found']}</code>
├─ 🔴 <b>High Risk:</b> <code>{security_scans['high_risk_files']}</code>
└─ 🚫 <b>Blocked:</b> <code>{security_scans['blocked_files']}</code>

<b>📈 ADMIN:</b>
├─ 🔔 <b>Alerts:</b> <code>{stats['security_alerts']}</code>
└─ 🚫 <b>Banned:</b> <code>{stats['banned_users']}</code>

<b>⚡ Status:</b> 🟢 Online
<b>🔒 Mode:</b> {'🔒 Locked' if bot_locked else '🔓 Open'}
<b>📈 Limit:</b> {FREE_USER_LIMIT}
<b>🔰 Community:</b> {'✅ On' if force_join_enabled else '❌ Off'}
    """
    
    bot.send_message(message.chat.id, html_text, parse_mode='HTML')

def handle_premium_users_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    premium_users = get_premium_users_details()
    if not premium_users:
        bot.send_message(message.chat.id, "📭 No Premium Users")
        return
    
    html_text = "<b>💎 PREMIUM USERS LIST</b>\n\n"
    
    for user in premium_users:
        days_left = (user['expiry'] - datetime.now()).days
        
        html_text += f"""
<b>👤 {user['first_name']}</b> (@{user['username']})
├─ <b>ID:</b> <code>{user['user_id']}</code>
├─ <b>Files:</b> {user['file_count']}/{user['file_limit']} 
├─ <b>Running:</b> 🟢 {user['running_files']}
└─ <b>Days Left:</b> {days_left}
"""
    
    html_text += f"\n<b>📊 TOTAL PREMIUM:</b> {len(premium_users)} users"
    
    bot.send_message(message.chat.id, html_text, parse_mode='HTML')

def handle_broadcast_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    msg = bot.send_message(message.chat.id, "📢 Enter message:")
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    broadcast_messages[message.message_id] = message.text
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Send", callback_data=f'confirm_broadcast_{message.message_id}'),
        types.InlineKeyboardButton("❌ Cancel", callback_data='cancel_broadcast')
    )
    
    bot.send_message(message.chat.id, 
                    f"📢 **PREVIEW:**\n\n{message.text}\n\nSend to all users?",
                    reply_markup=markup, parse_mode='Markdown')

def handle_generate_key_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    msg = bot.send_message(message.chat.id, "📅 Duration (Days):")
    bot.register_next_step_handler(msg, process_generate_key_days)

def process_generate_key_days(message):
    try:
        days = int(message.text.strip())
        if days <= 0:
            bot.send_message(message.chat.id, "❌ Positive number required")
            return
        
        bot.send_message(message.chat.id, f"✅ {days} Days\n\nMax Uses:")
        bot.register_next_step_handler(message, process_generate_key_uses, days)
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Number required")

def process_generate_key_uses(message, days):
    try:
        max_uses = int(message.text.strip())
        if max_uses <= 0:
            bot.send_message(message.chat.id, "❌ Positive number required")
            return
        
        bot.send_message(message.chat.id, f"🗃 File Limit (1-999):")
        bot.register_next_step_handler(message, process_generate_key_file_limit, days, max_uses)
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Number required")

def process_generate_key_file_limit(message, days, max_uses):
    try:
        file_limit = int(message.text.strip())
        if file_limit < 1 or file_limit > 999:
            bot.send_message(message.chat.id, "❌ 1-999")
            return
        
        key = generate_subscription_key(days, max_uses, file_limit, created_by=message.from_user.id)
        bot.send_message(message.chat.id, 
                        f"""
✅ **KEY GENERATED**

🔑 `{key}`
📅 {days} Days
🗃 {file_limit} Files
🔢 {max_uses} Uses
                        """,
                        parse_mode='Markdown')
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Number required")

def handle_delete_key_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    keys = get_all_subscription_keys()
    if not keys:
        bot.send_message(message.chat.id, "📭 No Keys")
        return
    
    keys_text = f"🗑️ **ACTIVE KEYS:**\n\n"
    for key in keys:
        keys_text += f"• `{key[0]}` - {key[1]}d, {key[3]}/{key[2]}, {key[4]} files\n"
    
    keys_text += "\nEnter key to revoke:"
    bot.send_message(message.chat.id, keys_text, parse_mode='Markdown')
    
    msg = bot.send_message(message.chat.id, "🔑 Key:")
    bot.register_next_step_handler(msg, process_delete_key)

def process_delete_key(message):
    key_value = message.text.strip().upper()

    keys = get_all_subscription_keys()
    key_exists = any(key[0] == key_value for key in keys)
    
    if not key_exists:
        bot.send_message(message.chat.id, f"❌ `{key_value}` Not Found", parse_mode='Markdown')
        return
    
    delete_subscription_key(key_value)
    bot.send_message(message.chat.id, f"✅ `{key_value}` Revoked", parse_mode='Markdown')

def handle_total_keys_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    keys = get_all_subscription_keys()
    if not keys:
        bot.send_message(message.chat.id, "📭 No Keys")
        return
    
    keys_text = f"🔢 **ALL KEYS:**\n\n"
    for key in keys:
        keys_text += f"• `{key[0]}`\n  📅 {key[1]}d, 📊 {key[4]} files, 🔢 {key[3]}/{key[2]}\n  🕐 {key[5][:16]}\n\n"
    
    bot.send_message(message.chat.id, keys_text, parse_mode='Markdown')

def handle_file_limit_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    current_limit = FREE_USER_LIMIT
    msg = bot.send_message(message.chat.id, f"📈 Current Limit: {current_limit}\n\nNew Limit (1-100):")
    bot.register_next_step_handler(msg, process_file_limit)

def process_file_limit(message):
    try:
        new_limit = int(message.text.strip())
        if 1 <= new_limit <= 100:
            update_file_limit(new_limit)
            bot.send_message(message.chat.id, f"✅ Limit: {new_limit}")
        else:
            bot.send_message(message.chat.id, "❌ 1-100")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Number")

def handle_key_user_info_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    msg = bot.send_message(message.chat.id, "<b>🔑 Enter key to check:</b>", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_key_user_info_html)

def process_key_user_info_html(message):
    key_value = message.text.strip().upper()
    
    user_info = get_user_by_key(key_value)
    
    if not user_info:
        bot.reply_to(message, f"❌ No user found for key <code>{key_value}</code>", parse_mode='HTML')
        return
    
    html_text = f"""
<b>🔑 KEY-USER INFORMATION</b>

<b>Key:</b> <code>{key_value}</code>

<b>👤 USER DETAILS:</b>
├─ <b>ID:</b> <code>{user_info['user_id']}</code>
├─ <b>Name:</b> {user_info['first_name']}
├─ <b>Username:</b> @{user_info['username'] if user_info['username'] else 'N/A'}
├─ <b>Duration:</b> {user_info['days_valid']} Days
├─ <b>File Limit:</b> {user_info['file_limit']}
├─ <b>Key Activated:</b> {user_info['key_activation_date'][:19]}
└─ <b>User Data Saved:</b> {user_info['key_used_date'][:19]}

<b>📝 Note:</b> 1Key = 1User
    """
    
    user_files_list = get_user_files_with_details(user_info['user_id'])
    
    if user_files_list:
        html_text += f"\n<b>📁 FILES ({len(user_files_list)}):</b>\n"
        for file in user_files_list[:10]: 
            status = "🟢" if file['is_running'] else "🔴"
            html_text += f"├─ {status} <code>{file['file_name']}</code> ({file['file_size']})\n"
        
        if len(user_files_list) > 10:
            html_text += f"└─ <b>... {len(user_files_list) - 10} more files</b>\n"
    else:
        html_text += "\n<b>📭 NO FILES</b>"
    
    bot.reply_to(message, html_text, parse_mode='HTML')

def handle_all_users_text(message):
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    users = get_all_users_details()
    if not users:
        bot.send_message(message.chat.id, "📭 No Users")
        return
    
    html_text = "<b>👥 USERS LIST</b>\n\n"
    
    for user in users[:50]:  
        status = "💎" if user['is_premium'] else "👤"
        username = f"@{user['username']}" if user['username'] else "-"
        
        html_text += f"""
{status} <b>{user['first_name']}</b>
├─ <b>ID:</b> <code>{user['user_id']}</code>
└─ <b>Username:</b> {username}
"""
        html_text += "─" * 25 + "\n"
    
    if len(users) > 50:
        html_text += f"\n<b>📈 ... {len(users) - 50} more users</b>"
    
    html_text += f"\n<b>📊 TOTAL USERS:</b> {len(users)}"
    
    bot.send_message(message.chat.id, html_text, parse_mode='HTML')

def handle_back_to_main_text(message):
    user_id = message.from_user.id
    markup = create_main_menu_keyboard(user_id)
    bot.send_message(message.chat.id, "⬅️ Main Menu", reply_markup=markup)

def handle_my_info_text(message):
    user_id = message.from_user.id
    user_status = get_user_status(user_id)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    subscription_info = ""
    if is_premium_user(user_id):
        subscription_data = user_subscriptions.get(user_id, {})
        expiry = subscription_data.get('expiry', datetime.now())
        file_limit = subscription_data.get('file_limit', 999)
        days_left = (expiry - datetime.now()).days
        subscription_info = f"📅 Expires: {expiry.strftime('%Y-%m-%d')}\n📊 Limit: {file_limit} Files\n⏳ Days Left: {days_left}"
    else:
        subscription_info = "⏳ Standard Plan"
    
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    
    my_info_text = f"""
👤 **PROFILE**

🤖 ID: `{user_id}`
👤 Name: {message.from_user.first_name}
📱 Username: @{message.from_user.username if message.from_user.username else '-'}
📊 Status: {user_status}
---------------------------------------------

💎 **Tier:**
{subscription_info}
📂 Used: {current_files}/{limit_str}
---------------------------------------------

📁 **Files:**
├─ 🗃 Total: {current_files}
├─ 🟢 Active: {sum(1 for fn, _, _ in user_files.get(user_id, []) if is_bot_running(user_id, fn))}
└─ 🔴 Stopped: {sum(1 for fn, _, _ in user_files.get(user_id, []) if not is_bot_running(user_id, fn))}
---------------------------------------------
    """
    
    markup = types.InlineKeyboardMarkup()
    if not is_premium_user(user_id):
        markup.add(types.InlineKeyboardButton("💎 Upgrade", callback_data='buy_subscription'))
    markup.add(types.InlineKeyboardButton("📁 Files", callback_data='manage_files'))
    markup.add(types.InlineKeyboardButton("🔑 Redeem", callback_data='redeem_key'))
    
    bot.send_message(message.chat.id, my_info_text, reply_markup=markup, parse_mode='Markdown')

def handle_status_text(message):
    user_id = message.from_user.id
    user_status = get_user_status(user_id)
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    
    status_text = f"""
📊 **CURRENT STATUS**

👤User: {message.from_user.first_name}
📊Status: {user_status}
📁Files: {current_files}/{file_limit if file_limit != float('inf') else 'Unlimited'}
🟢Running: {sum(1 for fn, _, _ in user_files.get(user_id, []) if is_bot_running(user_id, fn))}
🔴Stopped: {sum(1 for fn, _, _ in user_files.get(user_id, []) if not is_bot_running(user_id, fn))}

💎Premium: {'Active' if is_premium_user(user_id) else 'Basic'}
🔒Bot Status: {'Locked' if bot_locked else 'Open'}
🔰Force Join: {'On' if force_join_enabled else 'Off'}
    """
    
    bot.send_message(message.chat.id, status_text, parse_mode='Markdown')

# --- Ban User ---
def handle_ban_user_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    msg = bot.send_message(message.chat.id, "🆔 Enter user ID to ban:")
    bot.register_next_step_handler(msg, process_ban_user)

def process_ban_user(message):
    try:
        user_id = int(message.text.strip())
        
        if user_id == OWNER_ID:
            bot.send_message(message.chat.id, "❌ Can't ban Owner")
            return
        
        if user_id in admin_ids:
            bot.send_message(message.chat.id, "❌ Can't ban Admin\nRemove admin first")
            return
        
        try:
            user_info = bot.get_chat(user_id)
            username = f"@{user_info.username}" if user_info.username else "N/A"
            name = user_info.first_name
        except:
            username = "N/A"
            name = "Unknown"
        
        success, result = ban_user(user_id)
        
        if success:
            try:
                bot.send_message(user_id,
                               """
🚫 <b>YOU HAVE BEEN BANNED</b>

⚠️ Your access has been revoked
📁 All your files have been deleted

👑 <b>Contact:</b> """ + YOUR_USERNAME + """
                               """,
                               parse_mode='HTML')
            except:
                pass
            
            bot.send_message(message.chat.id,
                           f"""
✅ <b>USER BANNED</b>

👤 {name}
🆔 <code>{user_id}</code>
👥 {username}
🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

📝 <b>ACTIONS TAKEN:</b>
• Removed from active users
• Deleted all files
• Killed running processes
• Revoked subscription
                           """,
                           parse_mode='HTML')
        else:
            bot.send_message(message.chat.id, f"❌ {result}")
            
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid ID")

def handle_unban_user_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    banned_users = get_banned_users()
    if not banned_users:
        bot.send_message(message.chat.id, "📭 No Banned Users")
        return
    
    banned_text = "🚫 **BANNED USERS:**\n\n"
    for user in banned_users:
        user_id, ban_date, reason, username, first_name, last_name = user
        name = first_name or "Unknown"
        username_display = f"@{username}" if username else "N/A"
        banned_text += f"• `{user_id}` - {name} ({username_display})\n"
    
    banned_text += "\n🆔 Enter user ID to unban:"
    msg = bot.send_message(message.chat.id, banned_text, parse_mode='Markdown')
    bot.register_next_step_handler(msg, process_unban_user)

def process_unban_user(message):
    try:
        user_id = int(message.text.strip())
        
        success, result = unban_user(user_id)
        
        if success:
            try:
                bot.send_message(user_id,
                               f"""
✅ *YOU HAVE BEEN UNBANNED*

✨ Your access has been restored
Use /start to begin again
                               """,
                               parse_mode='Markdown')
            except:
                pass
            
            bot.send_message(message.chat.id, f"✅ User `{user_id}` Unbanned", parse_mode='Markdown')
        else:
            bot.send_message(message.chat.id, f"❌ {result}")
            
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid ID")

def handle_view_banned_users_text(message):
    if message.from_user.id != OWNER_ID:
        bot.send_message(message.chat.id, "❌ Owner Only")
        return
    
    banned_users = get_banned_users()
    if not banned_users:
        bot.send_message(message.chat.id, "📭 No Banned Users")
        return
    
    html_text = "<b>🚫 BANNED USERS LIST</b>\n\n"
    
    for user in banned_users:
        user_id, ban_date, reason, username, first_name, last_name = user
        name = first_name or "Unknown"
        username_display = f"@{username}" if username else "N/A"
        time_ago = (datetime.now() - datetime.fromisoformat(ban_date)).days
        
        html_text += f"""
<b>👤 User:</b> {name}
<b>🆔 ID:</b> <code>{user_id}</code>
<b>📱 Username:</b> {username_display}
<b>📅 Banned:</b> {ban_date[:16]} ({time_ago} days ago)
"""
        if reason:
            html_text += f"<b>📝 Reason:</b> {reason}\n"
        html_text += "─" * 30 + "\n"
    
    html_text += f"\n<b>📊 TOTAL:</b> {len(banned_users)} users"
    
    bot.send_message(message.chat.id, html_text, parse_mode='HTML')

# --- Force Stop User ---
def handle_force_stop_user_text(message):
    """Force stop all processes for a specific user (Admin/Owner only)"""
    if message.from_user.id not in admin_ids:
        bot.send_message(message.chat.id, "❌ Admin Only")
        return
    
    msg = bot.send_message(message.chat.id, "🆔 Enter user ID to force stop all processes:")
    bot.register_next_step_handler(msg, process_force_stop_user)

def process_force_stop_user(message):
    try:
        user_id = int(message.text.strip())
        
        stopped_count = cleanup_user_processes(user_id)
        
        bot.send_message(message.chat.id, f"✅ Force stopped {stopped_count} processes for user {user_id}")
        
    except ValueError:
        bot.send_message(message.chat.id, "❌ Invalid ID")

# --- Security Action ---
@bot.callback_query_handler(func=lambda call: call.data.startswith('security_'))
def handle_security_actions(call):
    try:
        parts = call.data.split('_')
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "❌ Invalid action", show_alert=True)
            return
            
        action = parts[1]
        user_id_str = parts[2]
        file_name = '_'.join(parts[3:]) if len(parts) > 3 else "Unknown"
        
        user_id = int(user_id_str)
        
        if call.from_user.id != OWNER_ID:
            bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
            return
        
        if action == 'ban':
            success, result = ban_user(user_id)
            if success:
                log_security_event(user_id, "Unknown", file_name, 0, 'critical', 'banned')
                bot.answer_callback_query(call.id, f"✅ Banned {user_id}", show_alert=True)
                bot.edit_message_text(
                    f"✅ **USER BANNED**\n\n👤 User ID: `{user_id}`\n📁 File: `{file_name}`\n🕐 {datetime.now().strftime('%H:%M:%S')}",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
            else:
                bot.answer_callback_query(call.id, f"❌ {result}", show_alert=True)
        
        elif action == 'warn':
            if user_id not in user_warnings:
                user_warnings[user_id] = 0
            
            user_warnings[user_id] += 1
            warning_count = user_warnings[user_id]
            
            try:
                bot.send_message(user_id,
                    f"""⚠️ **SECURITY WARNING** ⚠️

Your uploaded file `{file_name}` has triggered security checks.
This is warning #{warning_count}.

Repeated violations may result in account suspension.
                    """,
                    parse_mode='Markdown'
                )
                
                if warning_count >= MAX_WARNINGS:
                    success, result = ban_user(user_id)
                    if success:
                        bot.send_message(user_id,
                                       """
🚫 <b>ACCOUNT BANNED</b>

⚠️ You have exceeded the warning limit
📁 All your files have been deleted

👑 <b>Contact:</b> """ + YOUR_USERNAME + """
                                       """,
                                       parse_mode='HTML')
                        bot.answer_callback_query(call.id, f"🚫 Auto-banned {user_id} (3/3)", show_alert=True)
                        bot.edit_message_text(
                            f"🚫 **AUTO-BANNED**\n\n👤 User ID: `{user_id}`\n📁 File: `{file_name}`\n⚠️ Warning #{warning_count}/{MAX_WARNINGS}",
                            call.message.chat.id,
                            call.message.message_id,
                            parse_mode='Markdown'
                        )
                    else:
                        bot.answer_callback_query(call.id, f"❌ {result}", show_alert=True)
                else:
                    bot.answer_callback_query(call.id, f"⚠️ Warned {user_id} ({warning_count}/{MAX_WARNINGS})", show_alert=True)
                    bot.edit_message_text(
                        f"⚠️ **USER WARNED**\n\n👤 User ID: `{user_id}`\n📁 File: `{file_name}`\n⚠️ Warning #{warning_count}/{MAX_WARNINGS}",
                        call.message.chat.id,
                        call.message.message_id,
                        parse_mode='Markdown'
                    )
            except:
                bot.answer_callback_query(call.id, "❌ Can't message user", show_alert=True)
        
        elif action == 'delete':
            remove_user_file_db(user_id, file_name)
            log_security_event(user_id, "Unknown", file_name, 0, 'medium', 'file_deleted')
            bot.answer_callback_query(call.id, f"🗑️ Deleted {file_name}", show_alert=True)
            bot.edit_message_text(
                f"🗑️ **FILE DELETED**\n\n📁 File: `{file_name}`\n👤 User ID: `{user_id}`",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
        
        elif action == 'ignore':
            # Approve the pending file
            approve_pending_file(user_id, file_name)
            
            log_security_event(user_id, "Unknown", file_name, 0, 'low', 'ignored')
            bot.answer_callback_query(call.id, "👁️ Ignored", show_alert=True)
            bot.edit_message_text(
                f"👁️ **THREAT IGNORED**\n\n📁 File: `{file_name}`\n👤 User ID: `{user_id}`\n✅ Deployment allowed",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            
            try:
                user_info = bot.get_chat(user_id)
                user_mention = f"[{user_info.first_name}](tg://user?id={user_id})" if user_info.first_name else f"User {user_id}"
                
                deploy_msg = f"""
🚀 **DEPLOYMENT ALLOWED** 🚀

Your file `{file_name}` has been approved for deployment.
Tap the button below to start hosting:

🔧 **File Details:**
• Name: `{file_name}`
• Type: {SUPPORTED_EXTENSIONS.get(os.path.splitext(file_name)[1].lower(), 'Unknown')}
• Status: ✅ Approved

📊 **Your Usage:**
• Files: {get_user_file_count(user_id)}/{get_user_file_limit(user_id)}
• Status: {get_user_status(user_id)}
                """
                
                deploy_markup = types.InlineKeyboardMarkup()
                deploy_markup.add(types.InlineKeyboardButton("🚀 Deploy Now", callback_data=f'deploy_{user_id}_{file_name}'))
                
                bot.send_message(user_id, deploy_msg, reply_markup=deploy_markup, parse_mode='Markdown')
                
            except Exception as e:
                logger.error(f"❌ Error sending deploy message: {e}")
        
        elif action == 'block':
            success, result = ban_user(user_id)
            if success:
                log_security_event(user_id, "Unknown", file_name, 0, 'high', 'blocked_uploads')
                bot.answer_callback_query(call.id, f"🔒 Blocked {user_id}", show_alert=True)
                bot.edit_message_text(
                    f"🔒 **UPLOADS BLOCKED**\n\n👤 User ID: `{user_id}`\n📁 File: `{file_name}`\n🚫 Future uploads disabled",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='Markdown'
                )
        
        elif action == 'report':
            bot.answer_callback_query(call.id, "📋 Report shown", show_alert=False)
    
    except Exception as e:
        logger.error(f"❌ Error in security action: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith('deploy_'))
def handle_deploy_callback(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if call.from_user.id != user_id:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        file_path = None
        for fn, ft, fp in user_files.get(user_id, []):
            if fn == file_name:
                file_path = fp
                break
        
        if not file_path or not os.path.exists(file_path):
            bot.answer_callback_query(call.id, "❌ Not Found", show_alert=True)
            return
        
        user_folder = get_user_folder(user_id)
        file_ext = os.path.splitext(file_name)[1].lower()
        
        if file_ext == '.py':
            threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            bot.answer_callback_query(call.id, f"🚀 Starting...")
        elif file_ext == '.js':
            threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            bot.answer_callback_query(call.id, f"🚀 Starting...")
        else:
            bot.answer_callback_query(call.id, f"✅ Deployed")
        
        time.sleep(1)
        handle_file_click(call)
        
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)        

# --- Callback Query Handlers ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    user_id = call.from_user.id

    if call.message.chat.type in ['group', 'supergroup']:
        bot.answer_callback_query(call.id, "❌ This bot only works in private chats", show_alert=True)
        return

    if not (user_id in admin_ids or user_id == OWNER_ID) and is_user_banned(user_id):
        bot.answer_callback_query(call.id,
                                 f"""
🚫 YOU ARE BANNED
⚠️ YOUR ACCESS HAS BEEN REVOKED
                                 """,
                                 show_alert=True)
        return

    if bot_locked and user_id not in admin_ids:
        bot.answer_callback_query(call.id, 
                                 f"🔒 MAINTENANCE MODE", 
                                 show_alert=True)
        return
    
    data = call.data
    
    try:
        if data == 'check_membership':
            handle_check_membership(call)
        elif data == 'start_hosting':
            handle_start_hosting_callback(call)
        elif data == 'manage_files':
            handle_manage_files_callback(call)
        elif data.startswith('file_'):
            handle_file_click(call)
        elif data == 'redeem_key':
            msg = bot.send_message(call.message.chat.id, "🔑 Enter Key:")
            bot.register_next_step_handler(msg, process_redeem_key)
        elif data == 'buy_subscription':
            handle_buy_subscription_text(call.message)
        elif data == 'admin_panel':
            handle_admin_panel_text(call.message)
        elif data == 'bot_statistics':
            handle_bot_statistics_text(call.message)
        elif data == 'all_users':
            handle_all_users_text(call.message)
        elif data == 'premium_users':
            handle_premium_users_text(call.message)
        elif data == 'broadcast':
            handle_broadcast_text(call.message)
        elif data == 'generate_key':
            handle_generate_key_text(call.message)
        elif data == 'delete_key':
            handle_delete_key_text(call.message)
        elif data == 'total_keys':
            handle_total_keys_text(call.message)
        elif data == 'bot_settings':
            handle_bot_settings_text(call.message)
        elif data == 'back_to_main':
            handle_back_to_main_callback(call)
        elif data.startswith('start_'):
            handle_start_file(call)
        elif data.startswith('stop_'):
            handle_stop_file(call)
        elif data.startswith('restart_'):
            handle_restart_file(call)
        elif data.startswith('delete_'):
            handle_delete_file(call)
        elif data.startswith('logs_'):
            handle_logs_file(call)
        elif data.startswith('confirm_broadcast_'):
            handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast':
            handle_cancel_broadcast(call)
        elif data == 'no_files':
            bot.answer_callback_query(call.id, "📭 No Files", show_alert=True)
        elif data.startswith('security_'):
            handle_security_actions(call)
            
    except Exception as e:
        logger.error(f"❌ Error in callback handler: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

def handle_check_membership(call):
    user_id = call.from_user.id
    
    if user_id in admin_ids:
        bot.answer_callback_query(call.id, "✅ Admin Access", show_alert=True)
        return
    
    if check_force_join(user_id):
        bot.answer_callback_query(call.id, "✅ Verified", show_alert=True)
        
        add_active_user(user_id)
        save_user(user_id, call.from_user.username, call.from_user.first_name, call.from_user.last_name)
        
        welcome_text = f"""
☁️ **KELVIN VPS HOST** ☁️

✨ *Welcome, {call.from_user.first_name}!*

✅ **MEMBERSHIP VERIFIED**

📊 **Status:** {get_user_status(user_id)}
🗃 **Files:** {get_user_file_count(user_id)}/{get_user_file_limit(user_id) if get_user_file_limit(user_id) != float('inf') else 'Unlimited'}

Tap buttons to start
        """
        
        markup = create_main_menu_keyboard(user_id)

        try:
            bot.send_message(call.message.chat.id, welcome_text, reply_markup=markup, parse_mode='Markdown')
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.error(f"❌ Error sending welcome message: {e}")
            try:
                bot.edit_message_text(welcome_text, call.message.chat.id, call.message.message_id, 
                                     reply_markup=markup, parse_mode='Markdown')
            except Exception as e2:
                logger.error(f"❌ Error editing message: {e2}")
                bot.send_message(call.message.chat.id, welcome_text, reply_markup=markup, parse_mode='Markdown')
    else:
        bot.answer_callback_query(call.id, "❌ Join the group", show_alert=True)

def handle_manage_files_callback(call):
    user_id = call.from_user.id
    
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                             reply_markup=force_markup, parse_mode='Markdown')
        return
    
    user_files_list = user_files.get(user_id, [])
    
    if not user_files_list:
        bot.answer_callback_query(call.id, "📭 No Files", show_alert=True)
        return
    
    files_text = f"📂 **MY FILES:**\n\n"
    
    for file_name, file_type, file_path in user_files_list:
        is_running = is_bot_running(user_id, file_name)
        status = "🟢 Running" if is_running else "🔴 Stopped"
        files_text += f"• `{file_name}` - {status}\n"
    
    files_text += "\nTap file to manage"
    
    markup = create_manage_files_keyboard(user_id)
    bot.edit_message_text(files_text, call.message.chat.id, call.message.message_id, 
                         reply_markup=markup, parse_mode='Markdown')

def handle_file_click(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        file_details = None
        for fn, ft, fp in user_files.get(user_id, []):
            if fn == file_name:
                file_details = (fn, ft, fp)
                break
        
        if not file_details:
            bot.answer_callback_query(call.id, "❌ Not Found", show_alert=True)
            return
        
        file_name, file_type, file_path = file_details
        is_running = is_bot_running(user_id, file_name)
        
        file_text = f"""
FILE NAME: `{file_name}`

FILE TYPE: {file_type}
STATUS: {'🟢 Running' if is_running else '🔴 Stopped'}
        """
        
        markup = create_file_management_buttons(user_id, file_name, is_running)
        bot.edit_message_text(file_text, call.message.chat.id, call.message.message_id,
                             reply_markup=markup, parse_mode='Markdown')
        
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_start_hosting_callback(call):
    user_id = call.from_user.id
    
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                             reply_markup=force_markup, parse_mode='Markdown')
        return
    
    user_files_list = user_files.get(user_id, [])
    
    if not user_files_list:
        bot.answer_callback_query(call.id, "❌ No Files", show_alert=True)
        return
    
    bot.answer_callback_query(call.id, "🚀 Starting...")
    
    started_count = 0
    for file_name, file_type, file_path in user_files_list:
        if not is_bot_running(user_id, file_name):
            user_folder = get_user_folder(user_id)
            
            if os.path.exists(file_path):
                file_ext = os.path.splitext(file_name)[1].lower()
                if file_ext == '.py':
                    threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
                    started_count += 1
                elif file_ext == '.js':
                    threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
                    started_count += 1
                time.sleep(1)
    
    if started_count > 0:
        bot.send_message(call.message.chat.id, f"✅ Deployed {started_count} files")
    else:
        bot.send_message(call.message.chat.id, "ℹ️ All Active")

def handle_back_to_main_callback(call):
    user_id = call.from_user.id
    
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                             reply_markup=force_markup, parse_mode='Markdown')
        return
    
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    user_status = get_user_status(user_id)
    
    main_menu_text = f"""
☁️ **KELVIN CLOUD HOST**

👋 *{call.from_user.first_name}*

🤖 `{user_id}`
📊 {user_status}
📁 {current_files} / {limit_str}
    """
    
    markup = create_main_menu_keyboard(user_id)
    bot.edit_message_text(main_menu_text, call.message.chat.id, call.message.message_id, 
                         reply_markup=markup, parse_mode='Markdown')

def handle_start_file(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        file_path = None
        for fn, ft, fp in user_files.get(user_id, []):
            if fn == file_name:
                file_path = fp
                break
        
        if not file_path or not os.path.exists(file_path):
            bot.answer_callback_query(call.id, "❌ Not Found", show_alert=True)
            return
        
        user_folder = get_user_folder(user_id)
        file_ext = os.path.splitext(file_name)[1].lower()
        
        if file_ext == '.py':
            threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            bot.answer_callback_query(call.id, f"🚀 Starting...")
        elif file_ext == '.js':
            threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            bot.answer_callback_query(call.id, f"🚀 Starting...")
        else:
            bot.answer_callback_query(call.id, f"✅ Deployed")
        
        time.sleep(1)
        handle_file_click(call)
        
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_stop_file(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        script_key = f"{user_id}_{file_name}"
        
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        process_info = bot_scripts.get(script_key)
        if process_info:
            success = force_cleanup_process(process_info)
            
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            
            if success:
                bot.answer_callback_query(call.id, f"⏸️ Stopped")
            else:
                bot.answer_callback_query(call.id, f"⚠️ Partially stopped")
        else:
            bot.answer_callback_query(call.id, f"ℹ️ Not Running")
        
        time.sleep(1)
        handle_file_click(call)
            
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_restart_file(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        script_key = f"{user_id}_{file_name}"
        process_info = bot_scripts.get(script_key)
        if process_info:
            force_cleanup_process(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            time.sleep(1)
        
        file_path = None
        for fn, ft, fp in user_files.get(user_id, []):
            if fn == file_name:
                file_path = fp
                break
        
        if file_path and os.path.exists(file_path):
            user_folder = get_user_folder(user_id)
            file_ext = os.path.splitext(file_name)[1].lower()
            if file_ext == '.py':
                threading.Thread(target=run_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            elif file_ext == '.js':
                threading.Thread(target=run_js_script, args=(file_path, user_id, user_folder, file_name, call.message)).start()
            bot.answer_callback_query(call.id, f"🔄 Restarting")
        else:
            bot.answer_callback_query(call.id, "❌ Not Found", show_alert=True)
        
        time.sleep(1)
        handle_file_click(call)
            
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_delete_file(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "❌ Denied", show_alert=True)
            return
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        file_path = None
        file_type = None
        for fn, ft, fp in user_files.get(user_id, []):
            if fn == file_name:
                file_path = fp
                file_type = ft
                break
        
        if not file_path:
            bot.answer_callback_query(call.id, "❌ Not Found", show_alert=True)
            return
        
        script_key = f"{user_id}_{file_name}"
        process_info = bot_scripts.get(script_key)
        if process_info:
            force_cleanup_process(process_info)
            if script_key in bot_scripts:
                del bot_scripts[script_key]
        
        remove_user_file_db(user_id, file_name)
        
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"✅ Deleted file: {file_path}")
            except Exception as e:
                logger.error(f"❌ Error deleting file {file_path}: {e}")
        
        user_folder = get_user_folder(user_id)
        log_file = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        if os.path.exists(log_file):
            try:
                os.remove(log_file)
                logger.info(f"✅ Deleted log file: {log_file}")
            except Exception as e:
                logger.error(f"❌ Error deleting log file {log_file}: {e}")
        
        if user_id in user_files:
            user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
            if not user_files[user_id]:  
                del user_files[user_id]
        
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception as e:
            logger.error(f"❌ Error deleting message: {e}")
            bot.edit_message_text(
                f"✅ **{file_name}** Deleted Successfully",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='Markdown'
            )
            return 
        
        bot.send_message(
            call.message.chat.id,
            f"🗑️ **{file_name}** Deleted Successfully",
            parse_mode='Markdown'
        )
        
        handle_manage_files_callback(call)
        
    except Exception as e:
        logger.error(f"❌ Error in handle_delete_file: {e}")
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)


def handle_logs_file(call):
    try:
        _, user_id_str, file_name = call.data.split('_', 2)
        user_id = int(user_id_str)
        
        if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
            force_message = create_force_join_message()
            force_markup = create_force_join_keyboard()
            bot.edit_message_text(force_message, call.message.chat.id, call.message.message_id, 
                                 reply_markup=force_markup, parse_mode='Markdown')
            return
        
        user_folder = get_user_folder(user_id)
        log_file = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                logs = f.read()
            
            if len(logs) > 4000:
                logs = logs[:4000] + "\n\n... (Truncated)"
            
            log_text = f"📋 **{file_name}:**\n\n```\n{logs}\n```"
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Back", callback_data=f'file_{user_id}_{file_name}'))
            
            bot.edit_message_text(log_text, call.message.chat.id, call.message.message_id, 
                                 reply_markup=markup, parse_mode='Markdown')
        else:
            bot.answer_callback_query(call.id, "📭 No Logs", show_alert=True)
            
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_lock_bot(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    global bot_locked
    bot_locked = True
    bot.answer_callback_query(call.id, "🔒 Locked")
    bot.edit_message_text("🔒 **LOCKED**", 
                         call.message.chat.id, call.message.message_id, parse_mode='Markdown')

def handle_unlock_bot(call):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "❌ Owner Only", show_alert=True)
        return
    
    global bot_locked
    bot_locked = False
    bot.answer_callback_query(call.id, "🔓 Unlocked")
    bot.edit_message_text("🔓 **UNLOCKED**", 
                         call.message.chat.id, call.message.message_id, parse_mode='Markdown')

def handle_confirm_broadcast(call):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "❌ Admin Only", show_alert=True)
        return
    
    try:
        message_id = int(call.data.split('_')[2])
        
        if message_id in broadcast_messages:
            broadcast_text = broadcast_messages[message_id]
        else:
            bot.answer_callback_query(call.id, "❌ Message not found", show_alert=True)
            return
        
        sent_count = 0
        failed_count = 0
        
        for user_id in active_users:
            try:
                bot.send_message(user_id, broadcast_text)
                sent_count += 1
                time.sleep(0.1)
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ Failed to send to {user_id}: {e}")
        
        bot.answer_callback_query(call.id, f"✅ Sent: {sent_count}, Failed: {failed_count}")
        bot.edit_message_text(f"📢 Complete\n✅ {sent_count}\n❌ {failed_count}", 
                             call.message.chat.id, call.message.message_id)
        
        if message_id in broadcast_messages:
            del broadcast_messages[message_id]
        
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ {str(e)}", show_alert=True)

def handle_cancel_broadcast(call):
    try:
        message_id = int(call.data.split('_')[2]) if '_' in call.data else None
        
        if message_id and message_id in broadcast_messages:
            del broadcast_messages[message_id]
            
        bot.answer_callback_query(call.id, "❌ Cancelled")
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"❌ Error in cancel broadcast: {e}")

def process_redeem_key(message):
    user_id = message.from_user.id
    
    if force_join_enabled and user_id not in admin_ids and not check_force_join(user_id):
        force_message = create_force_join_message()
        force_markup = create_force_join_keyboard()
        bot.send_message(message.chat.id, force_message, reply_markup=force_markup, parse_mode=None)
        return
    
    key_value = message.text.strip().upper()
    
    if not key_value.startswith('KELVIN-') or len(key_value) != 16:
        bot.reply_to(message, f"❌ FORMAT: KELVIN-XXXX-XXXX\nEXAMPLE: KELVIN-A1B2-C3D4")
        return
    
    success, result_msg = redeem_subscription_key(key_value, user_id)
    bot.reply_to(message, result_msg)

def cleanup():
    logger.warning("🛑 Shutting down...")
    for script_key in list(bot_scripts.keys()):
        if script_key in bot_scripts:
            force_cleanup_process(bot_scripts[script_key])

atexit.register(cleanup)

def cleanup_zombie_processes():
    """Clean up any zombie processes that might still be running"""
    for script_key in list(bot_scripts.keys()):
        try:
            script_info = bot_scripts.get(script_key)
            if script_info and script_info.get('process'):
                pid = script_info['process'].pid
                try:
                    proc = psutil.Process(pid)
                    if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
                        if script_info.get('log_file'):
                            try:
                                script_info['log_file'].close()
                            except:
                                pass
                        del bot_scripts[script_key]
                except psutil.NoSuchProcess:
                    if script_info.get('log_file'):
                        try:
                            script_info['log_file'].close()
                        except:
                            pass
                    del bot_scripts[script_key]
        except Exception as e:
            logger.error(f"Error in cleanup: {e}")

def schedule_cleanup():
    """Regular cleanup of zombie processes and orphaned files"""
    while True:
        try:
            cleanup_zombie_processes()
            
            for script_key in list(bot_scripts.keys()):
                try:
                    script_info = bot_scripts.get(script_key)
                    if script_info and script_info.get('process'):
                        pid = script_info['process'].pid
                        try:
                            proc = psutil.Process(pid)
                            if not proc.is_running():
                                if script_key in bot_scripts:
                                    if script_info.get('log_file'):
                                        try:
                                            script_info['log_file'].close()
                                        except:
                                            pass
                                    del bot_scripts[script_key]
                        except psutil.NoSuchProcess:
                            # Process doesn't exist anymore
                            if script_key in bot_scripts:
                                if script_info.get('log_file'):
                                    try:
                                        script_info['log_file'].close()
                                    except:
                                        pass
                                del bot_scripts[script_key]
                except Exception as e:
                    logger.error(f"Error checking process {script_key}: {e}")
            
            for user_folder in os.listdir(UPLOAD_BOTS_DIR):
                user_folder_path = os.path.join(UPLOAD_BOTS_DIR, user_folder)
                if os.path.isdir(user_folder_path):
                    for file in os.listdir(user_folder_path):
                        if file.endswith('.log'):
                            log_path = os.path.join(user_folder_path, file)
                            if os.path.getmtime(log_path) < time.time() - 3600:
                                try:
                                    os.remove(log_path)
                                    logger.info(f"Cleaned up orphaned log file: {log_path}")
                                except:
                                    pass
            
            time.sleep(300)  # Run every 5 minutes
            
        except Exception as e:
            logger.error(f"Error in schedule_cleanup: {e}")
            time.sleep(60)

if __name__ == '__main__':
    cleanup_thread = threading.Thread(target=schedule_cleanup, daemon=True)
    cleanup_thread.start()
    keep_alive()
    logger.info("🚀 KELVIN Cloud Bot starting...")
    bot.polling(none_stop=True)
