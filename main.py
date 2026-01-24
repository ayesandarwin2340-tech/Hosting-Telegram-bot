# -*- coding: utf-8 -*-
import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import logging
# Removed unused telegram.* imports as we are using telebot consistently
# from telegram import Update
# from telegram.ext import Updater, CommandHandler, CallbackContext
import psutil
import sqlite3
import threading
import re # Added for regex matching in auto-install
import sys # Added for sys.executable
import atexit
import requests # For polling exceptions

# --- Flask Keep Alive ---
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "I'm Marco File Host"

def run_flask():
  # Make sure to run on port provided by environment or default to 8080
  port = int(os.environ.get("PORT", 8080))
  app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True # Allows program to exit even if this thread is running
    t.start()
    print("Flask Keep-Alive server started.")
# --- End Flask Keep Alive ---

# --- Configuration ---
TOKEN = '8391497992:AAGGCMLUrzPWtFkBgDvRTdRD0Faho9-V-N8' # Replace with your actual token
OWNER_ID = 6873534451 # Replace with your Owner ID
ADMIN_ID = 6873534451 # Replace with your Admin ID (can be same as Owner)
YOUR_USERNAME = '@Zinko158' # Replace with your Telegram username (without the @)
UPDATE_CHANNEL = 'https://t.me/+NLb-9NFUSiY1YjVl' # Replace with your update channel link

# Folder setup - using absolute paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__)) # Get script's directory
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf') # Assuming this name is intentional
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')

# File upload limits
FREE_USER_LIMIT = 5
SUBSCRIBED_USER_LIMIT = 15 # Changed from 10 to 15
ADMIN_LIMIT = 999       # Changed from 50 to 999
OWNER_LIMIT = float('inf') # Changed from 999 to infinity
# FREE_MODE_LIMIT = 3 # Removed as free_mode is removed

# Create necessary directories
os.makedirs(UPLOAD_BOTS_DIR, exist_ok=True)
os.makedirs(IROTECH_DIR, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {} # Stores info about running scripts {script_key: info_dict}
user_subscriptions = {} # {user_id: {'expiry': datetime_object}}
user_files = {} # {user_id: [(file_name, file_type), ...]}
active_users = set() # Set of all user IDs that have interacted with the bot
admin_ids = {ADMIN_ID, OWNER_ID} # Set of admin IDs
bot_locked = False
# free_mode = False # Removed free_mode

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Command Button Layouts (ReplyKeyboardMarkup) ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"], # Statistics button kept for users, logic will restrict if not admin
    ["📞 Contact Owner"]
]
ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["📢 Updates Channel"],
    ["📤 Upload File", "📂 Check Files"],
    ["⚡ Bot Speed", "📊 Statistics"],
    ["💳 Subscriptions", "📢 Broadcast"],
    ["🔒 Lock Bot", "🟢 Running All Code"], # Changed "Free Mode" to "Running All Code"
    ["👑 Admin Panel", "📞 Contact Owner"]
]

# --- Database Setup ---
def init_db():
    """Initialize the database with required tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False) # Allow access from multiple threads
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''') # Added admins table
        # Ensure owner and initial admin are in admins table
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
             c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}", exc_info=True)

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"⚠️ Invalid expiry date format for user {user_id}: {expiry}. Skipping.")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall()) # Load admins into the set

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_subscriptions)} subscriptions, {len(admin_ids)} admins.")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}", exc_info=True)

# Initialize DB and Load Data at startup
init_db()
load_data()
# --- End Database Setup ---

# --- Helper Functions ---
def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    # if free_mode: return FREE_MODE_LIMIT # Removed free_mode check
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name): # Parameter renamed for clarity
    """Check if a bot script is currently running for a specific user"""
    script_key = f"{script_owner_id}_{file_name}" # Key uses script_owner_id
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                logger.warning(f"Process {script_info['process'].pid} for {script_key} found in memory but not running/zombie. Cleaning up.")
                if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                    try:
                        script_info['log_file'].close()
                    except Exception as log_e:
                        logger.error(f"Error closing log file during zombie cleanup {script_key}: {log_e}")
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            logger.warning(f"Process for {script_key} not found (NoSuchProcess). Cleaning up.")
            if 'log_file' in script_info and hasattr(script_info['log_file'], 'close') and not script_info['log_file'].closed:
                try:
                     script_info['log_file'].close()
                except Exception as log_e:
                     logger.error(f"Error closing log file during cleanup of non-existent process {script_key}: {log_e}")
            if script_key in bot_scripts:
                 del bot_scripts[script_key]
            return False
        except Exception as e:
            logger.error(f"Error checking process status for {script_key}: {e}", exc_info=True)
            return False
    return False


def kill_process_tree(process_info):
    """Kill a process and all its children, ensuring log file is closed."""
    pid = None
    log_file_closed = False
    script_key = process_info.get('script_key', 'N/A')

    try:
        if 'log_file' in process_info and hasattr(process_info['log_file'], 'close') and not process_info['log_file'].closed:
            try:
                process_info['log_file'].close()
                log_file_closed = True
                logger.info(f"Closed log file for {script_key} (PID: {process_info.get('process', {}).get('pid', 'N/A')})")
            except Exception as log_e:
                logger.error(f"Error closing log file during kill for {script_key}: {log_e}")

        process = process_info.get('process')
        if process and hasattr(process, 'pid'):
           pid = process.pid
           if pid:
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    logger.info(f"Attempting to kill process tree for {script_key} (PID: {pid}, Children: {[c.pid for c in children]})")

                    for child in children:
                        try:
                            child.terminate()
                            logger.info(f"Terminated child process {child.pid} for {script_key}")
                        except psutil.NoSuchProcess:
                            logger.warning(f"Child process {child.pid} for {script_key} already gone.")
                        except Exception as e:
                            logger.error(f"Error terminating child {child.pid} for {script_key}: {e}. Trying kill...")
                            try: child.kill(); logger.info(f"Killed child process {child.pid} for {script_key}")
                            except Exception as e2: logger.error(f"Failed to kill child {child.pid} for {script_key}: {e2}")

                    gone, alive = psutil.wait_procs(children, timeout=1)
                    for p in alive:
                        logger.warning(f"Child process {p.pid} for {script_key} still alive. Killing.")
                        try: p.kill()
                        except Exception as e: logger.error(f"Failed to kill child {p.pid} for {script_key} after wait: {e}")

                    try:
                        parent.terminate()
                        logger.info(f"Terminated parent process {pid} for {script_key}")
                        try: parent.wait(timeout=1)
                        except psutil.TimeoutExpired:
                            logger.warning(f"Parent process {pid} for {script_key} did not terminate. Killing.")
                            parent.kill()
                            logger.info(f"Killed parent process {pid} for {script_key}")
                    except psutil.NoSuchProcess:
                        logger.warning(f"Parent process {pid} for {script_key} already gone.")
                    except Exception as e:
                        logger.error(f"Error terminating parent {pid} for {script_key}: {e}. Trying kill...")
                        try: parent.kill(); logger.info(f"Killed parent process {pid} for {script_key}")
                        except Exception as e2: logger.error(f"Failed to kill parent {pid} for {script_key}: {e2}")

                except psutil.NoSuchProcess:
                    logger.warning(f"Process {pid or 'N/A'} for {script_key} not found during kill. Already terminated?")
           else: logger.error(f"Process PID is None for {script_key}.")
        elif log_file_closed: logger.warning(f"Process object missing for {script_key}, but log file closed.")
        else: logger.error(f"Process object missing for {script_key}, and no log file. Cannot kill.")
    except Exception as e:
        logger.error(f"❌ Unexpected error killing process tree for PID {pid or 'N/A'} ({script_key}): {e}", exc_info=True)

# --- Automatic Package Installation & Script Running ---

def attempt_install_pip(module_name, message):
    package_name = TELEGRAM_MODULES.get(module_name.lower(), module_name)
    if package_name is None:
        logger.info(f"Module '{module_name}' is core. Skipping pip install.")
        return False
    try:
        bot.reply_to(message, f"🐍 Module `{module_name}` not found. Installing `{package_name}`...", parse_mode='Markdown')
        command = [sys.executable, '-m', 'pip', 'install', package_name]
        logger.info(f"Running install: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {package_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Package `{package_name}` (for `{module_name}`) installed.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install `{package_name}` for `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except Exception as e:
        error_msg = f"❌ Error installing `{package_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def attempt_install_npm(module_name, user_folder, message):
    try:
        bot.reply_to(message, f"🟠 Node package `{module_name}` not found. Installing locally...", parse_mode='Markdown')
        command = ['npm', 'install', module_name]
        logger.info(f"Running npm install: {' '.join(command)} in {user_folder}")
        result = subprocess.run(command, capture_output=True, text=True, check=False, cwd=user_folder, encoding='utf-8', errors='ignore')
        if result.returncode == 0:
            logger.info(f"Installed {module_name}. Output:\n{result.stdout}")
            bot.reply_to(message, f"✅ Node package `{module_name}` installed locally.", parse_mode='Markdown')
            return True
        else:
            error_msg = f"❌ Failed to install Node package `{module_name}`.\nLog:\n```\n{result.stderr or result.stdout}\n```"
            logger.error(error_msg)
            if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
            bot.reply_to(message, error_msg, parse_mode='Markdown')
            return False
    except FileNotFoundError:
         error_msg = "❌ Error: 'npm' not found. Ensure Node.js/npm are installed and in PATH."
         logger.error(error_msg)
         bot.reply_to(message, error_msg)
         return False
    except Exception as e:
        error_msg = f"❌ Error installing Node package `{module_name}`: {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message, error_msg)
        return False

def run_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run Python script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run Python script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = [sys.executable, script_path]
            logger.info(f"Running Python pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"Python Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_py = re.search(r"ModuleNotFoundError: No module named '(.+?)'", stderr)
                    if match_py:
                        module_name = match_py.group(1).strip().strip("'\"")
                        logger.info(f"Detected missing Python module: {module_name}")
                        if attempt_install_pip(module_name, message_obj_for_reply):
                            logger.info(f"Install OK for {module_name}. Retrying run_script...")
                            bot.reply_to(message_obj_for_reply, f"🔄 Install successful. Retrying '{file_name}'...")
                            time.sleep(2)
                            threading.Thread(target=run_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                            return
                        else:
                            bot.reply_to(message_obj_for_reply, f"❌ Install failed. Cannot run '{file_name}'.")
                            return
                    else:
                         error_summary = stderr[:500]
                         bot.reply_to(message_obj_for_reply, f"❌ Error in script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix the script.", parse_mode='Markdown')
                         return
            except subprocess.TimeoutExpired:
                logger.info("Python Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("Python Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 logger.error(f"Python interpreter not found: {sys.executable}")
                 bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
                 return
            except Exception as e:
                 logger.error(f"Error in Python pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in script pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"Python Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running Python process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
             logger.error(f"Failed to open log file '{log_file_path}' for {script_key}: {e}", exc_info=True)
             bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
             return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                [sys.executable, script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started Python process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies from script, defaults to admin/triggering user
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'py', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ Python script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             logger.error(f"Python interpreter {sys.executable} not found for long run {script_key}")
             bot.reply_to(message_obj_for_reply, f"❌ Error: Python interpreter '{sys.executable}' not found.")
             if log_file and not log_file.closed: log_file.close()
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting Python script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started Python process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running Python script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

def run_js_script(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt=1):
    """Run JS script. script_owner_id is used for the script_key. message_obj_for_reply is for sending feedback."""
    max_attempts = 2
    if attempt > max_attempts:
        bot.reply_to(message_obj_for_reply, f"❌ Failed to run '{file_name}' after {max_attempts} attempts. Check logs.")
        return

    script_key = f"{script_owner_id}_{file_name}"
    logger.info(f"Attempt {attempt} to run JS script: {script_path} (Key: {script_key}) for user {script_owner_id}")

    try:
        if not os.path.exists(script_path):
             bot.reply_to(message_obj_for_reply, f"❌ Error: Script '{file_name}' not found at '{script_path}'!")
             logger.error(f"JS Script not found: {script_path} for user {script_owner_id}")
             if script_owner_id in user_files:
                 user_files[script_owner_id] = [f for f in user_files.get(script_owner_id, []) if f[0] != file_name]
             remove_user_file_db(script_owner_id, file_name)
             return

        if attempt == 1:
            check_command = ['node', script_path]
            logger.info(f"Running JS pre-check: {' '.join(check_command)}")
            check_proc = None
            try:
                check_proc = subprocess.Popen(check_command, cwd=user_folder, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                stdout, stderr = check_proc.communicate(timeout=5)
                return_code = check_proc.returncode
                logger.info(f"JS Pre-check early. RC: {return_code}. Stderr: {stderr[:200]}...")
                if return_code != 0 and stderr:
                    match_js = re.search(r"Cannot find module '(.+?)'", stderr)
                    if match_js:
                        module_name = match_js.group(1).strip().strip("'\"")
                        if not module_name.startswith('.') and not module_name.startswith('/'):
                             logger.info(f"Detected missing Node module: {module_name}")
                             if attempt_install_npm(module_name, user_folder, message_obj_for_reply):
                                 logger.info(f"NPM Install OK for {module_name}. Retrying run_js_script...")
                                 bot.reply_to(message_obj_for_reply, f"🔄 NPM Install successful. Retrying '{file_name}'...")
                                 time.sleep(2)
                                 threading.Thread(target=run_js_script, args=(script_path, script_owner_id, user_folder, file_name, message_obj_for_reply, attempt + 1)).start()
                                 return
                             else:
                                 bot.reply_to(message_obj_for_reply, f"❌ NPM Install failed. Cannot run '{file_name}'.")
                                 return
                        else: logger.info(f"Skipping npm install for relative/core: {module_name}")
                    error_summary = stderr[:500]
                    bot.reply_to(message_obj_for_reply, f"❌ Error in JS script pre-check for '{file_name}':\n```\n{error_summary}\n```\nFix script or install manually.", parse_mode='Markdown')
                    return
            except subprocess.TimeoutExpired:
                logger.info("JS Pre-check timed out (>5s), imports likely OK. Killing check process.")
                if check_proc and check_proc.poll() is None: check_proc.kill(); check_proc.communicate()
                logger.info("JS Check process killed. Proceeding to long run.")
            except FileNotFoundError:
                 error_msg = "❌ Error: 'node' not found. Ensure Node.js is installed for JS files."
                 logger.error(error_msg)
                 bot.reply_to(message_obj_for_reply, error_msg)
                 return
            except Exception as e:
                 logger.error(f"Error in JS pre-check for {script_key}: {e}", exc_info=True)
                 bot.reply_to(message_obj_for_reply, f"❌ Unexpected error in JS pre-check for '{file_name}': {e}")
                 return
            finally:
                 if check_proc and check_proc.poll() is None:
                     logger.warning(f"JS Check process {check_proc.pid} still running. Killing.")
                     check_proc.kill(); check_proc.communicate()

        logger.info(f"Starting long-running JS process for {script_key}")
        log_file_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        log_file = None; process = None
        try: log_file = open(log_file_path, 'w', encoding='utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to open log file '{log_file_path}' for JS script {script_key}: {e}", exc_info=True)
            bot.reply_to(message_obj_for_reply, f"❌ Failed to open log file '{log_file_path}': {e}")
            return
        try:
            startupinfo = None; creationflags = 0
            if os.name == 'nt':
                 startupinfo = subprocess.STARTUPINFO(); startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                 startupinfo.wShowWindow = subprocess.SW_HIDE
            process = subprocess.Popen(
                ['node', script_path], cwd=user_folder, stdout=log_file, stderr=log_file,
                stdin=subprocess.PIPE, startupinfo=startupinfo, creationflags=creationflags,
                encoding='utf-8', errors='ignore'
            )
            logger.info(f"Started JS process {process.pid} for {script_key}")
            bot_scripts[script_key] = {
                'process': process, 'log_file': log_file, 'file_name': file_name,
                'chat_id': message_obj_for_reply.chat.id, # Chat ID for potential future direct replies
                'script_owner_id': script_owner_id, # Actual owner of the script
                'start_time': datetime.now(), 'user_folder': user_folder, 'type': 'js', 'script_key': script_key
            }
            bot.reply_to(message_obj_for_reply, f"✅ JS script '{file_name}' started! (PID: {process.pid}) (For User: {script_owner_id})")
        except FileNotFoundError:
             error_msg = "❌ Error: 'node' not found for long run. Ensure Node.js is installed."
             logger.error(error_msg)
             if log_file and not log_file.closed: log_file.close()
             bot.reply_to(message_obj_for_reply, error_msg)
             if script_key in bot_scripts: del bot_scripts[script_key]
        except Exception as e:
            if log_file and not log_file.closed: log_file.close()
            error_msg = f"❌ Error starting JS script '{file_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            bot.reply_to(message_obj_for_reply, error_msg)
            if process and process.poll() is None:
                 logger.warning(f"Killing potentially started JS process {process.pid} for {script_key}")
                 kill_process_tree({'process': process, 'log_file': log_file, 'script_key': script_key})
            if script_key in bot_scripts: del bot_scripts[script_key]
    except Exception as e:
        error_msg = f"❌ Unexpected error running JS script '{file_name}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        bot.reply_to(message_obj_for_reply, error_msg)
        if script_key in bot_scripts:
             logger.warning(f"Cleaning up {script_key} due to error in run_js_script.")
             kill_process_tree(bot_scripts[script_key])
             del bot_scripts[script_key]

# --- Map Telegram import names to actual PyPI package names ---
# --- Map Telegram import names to actual PyPI package names ---
TELEGRAM_MODULES = {
    # Main Bot Frameworks
    'telebot': 'pyTelegramBotAPI',
    'telegram': 'python-telegram-bot',
    'python_telegram_bot': 'python-telegram-bot',
    'aiogram': 'aiogram',
    'aiogram2': 'aiogram',
    'aiogram3': 'aiogram',
    'pyrogram': 'pyrogram',
    'telethon': 'telethon',
    'telethon.sync': 'telethon',
    'telethon.client': 'telethon',
    'telethon.events': 'telethon',
    'telethon.tl': 'telethon',
    'telegram.ext': 'python-telegram-bot',
    'telegram_bot': 'python-telegram-bot',
    
    # Additional Telegram Libraries
    'telepot': 'telepot',
    'pytg': 'pytg',
    'tgcrypto': 'tgcrypto',
    'telegram_upload': 'telegram-upload',
    'telegram_send': 'telegram-send',
    'telegram_text': 'telegram-text',
    'telethon_sync': 'telethon',
    'pytelegrambotapi': 'pyTelegramBotAPI',
    
    # MTProto & Low-Level
    'mtproto': 'telegram-mtproto',
    'tl': 'telethon',
    'tgcrypto': 'TgCrypto',
    'cryptg': 'cryptg',
    
    # Utilities & Helpers
    'telegram_utils': 'telegram-utils',
    'telegram_logger': 'telegram-logger',
    'telegram_handlers': 'python-telegram-handlers',
    'telegram_bot_pagination': 'telegram-bot-pagination',
    'telegram_bot_menu': 'telegram-bot-menu',
    'telegram_keyboard': 'telegram-keyboard',
    'telegram_inline': 'telegram-inline-keyboard',
    
    # Database Integrations
    'telegram_redis': 'telegram-redis',
    'telegram_sqlalchemy': 'telegram-sqlalchemy',
    'telegram_mongodb': 'telegram-mongodb',
    'telegram_peewee': 'telegram-peewee',
    
    # Payment & E-commerce
    'telegram_payment': 'telegram-payment',
    'telegram_shop': 'telegram-shop-sdk',
    'telegram_invoice': 'telegram-invoice',
    
    # Testing & Debugging
    'pytest_telegram': 'pytest-telegram',
    'telegram_debug': 'telegram-debug',
    'telegram_test': 'telegram-test',
    
    # Scraping & Analytics
    'telegram_scraper': 'telegram-scraper',
    'telegram_analytics': 'telegram-analytics',
    'telegram_stats': 'telegram-stats',
    
    # NLP & AI
    'telegram_nlp': 'telegram-nlp-toolkit',
    'telegram_ai': 'telegram-ai',
    'telegram_chatgpt': 'telegram-chatgpt',
    'telegram_openai': 'telegram-openai',
    
    # Web & API Integration
    'telegram_api': 'telegram-api-client',
    'telegram_web': 'telegram-web-integration',
    'telegram_rest': 'telegram-rest-api',
    'telegram_webhook': 'telegram-webhook',
    
    # Gaming & Interactive
    'telegram_games': 'telegram-games',
    'telegram_quiz': 'telegram-quiz-bot',
    'telegram_trivia': 'telegram-trivia',
    'telegram_casino': 'telegram-casino-bot',
    
    # File & Media Handling
    'telegram_ffmpeg': 'telegram-ffmpeg',
    'telegram_media': 'telegram-media-utils',
    'telegram_audio': 'telegram-audio-tools',
    'telegram_video': 'telegram-video-tools',
    'telegram_image': 'telegram-image-tools',
    
    # Security & Encryption
    'telegram_2fa': 'telegram-twofa',
    'telegram_crypto': 'telegram-crypto-bot',
    'telegram_security': 'telegram-security',
    
    # Localization & i18n
    'telegram_i18n': 'telegram-i18n',
    'telegram_translate': 'telegram-translate',
    'telegram_multilingual': 'telegram-multilingual',
    
    # Payment Gateways
    'telegram_stripe': 'telegram-stripe',
    'telegram_paypal': 'telegram-paypal',
    
    # Social Features
    'telegram_social': 'telegram-social',
    'telegram_profile': 'telegram-profile',
    'telegram_friends': 'telegram-friends-bot',
    
    # Scheduling & Reminders
    'telegram_scheduler': 'telegram-scheduler',
    'telegram_reminder': 'telegram-reminder',
    'telegram_cron': 'telegram-cron',
    
    # Weather & Location
    'telegram_weather': 'telegram-weather',
    'telegram_location': 'telegram-location',
    'telegram_map': 'telegram-map-bot',
    
    # News & RSS
    'telegram_news': 'telegram-news',
    'telegram_rss': 'telegram-rss',
    
    # Trading & Crypto
    'telegram_trading': 'telegram-trading-bot',
    'telegram_crypto_trading': 'telegram-crypto-trading',
    'telegram_binance': 'telegram-binance',
    
    # Education & Learning
    'telegram_edu': 'telegram-education',
    'telegram_quiz_master': 'telegram-quiz-master',
    
    # Health & Fitness
    'telegram_fitness': 'telegram-fitness',
    'telegram_diet': 'telegram-diet-bot',
    
    # Entertainment
    'telegram_music': 'telegram-music',
    'telegram_movie': 'telegram-movie-bot',
    
    # Business Tools
    'telegram_crm': 'telegram-crm',
    'telegram_support': 'telegram-support-bot',
    'telegram_ticket': 'telegram-ticket-system',
    
    # ===========================================
    # COMMON NON-TELEGRAM MODULES
    # ===========================================
    
    # Web & HTTP
    'requests': 'requests',
    'aiohttp': 'aiohttp',
    'httpx': 'httpx',
    'urllib': None,  # Core module
    'urllib2': None,  # Python 2, but included for compatibility
    'urllib3': 'urllib3',
    'urllib.request': None,  # Core module
    
    # Environment & Configuration
    'dotenv': 'python-dotenv',
    'python_dotenv': 'python-dotenv',
    'environs': 'environs',
    'configparser': None,  # Core module
    'yaml': 'PyYAML',
    'pyyaml': 'PyYAML',
    
    # Data Processing & Analysis
    'pandas': 'pandas',
    'numpy': 'numpy',
    'scipy': 'scipy',
    'matplotlib': 'matplotlib',
    'seaborn': 'seaborn',
    'plotly': 'plotly',
    
    # Database & ORM
    'sqlalchemy': 'SQLAlchemy',
    'peewee': 'peewee',
    'pymongo': 'pymongo',
    'motor': 'motor',  # Async MongoDB driver
    'redis': 'redis',
    'aioredis': 'aioredis',
    'psycopg2': 'psycopg2-binary',
    'mysql.connector': 'mysql-connector-python',
    'pymysql': 'pymysql',
    'sqlite3': None,  # Core module
    
    # Web Frameworks
    'flask': 'Flask',
    'django': 'Django',
    'fastapi': 'fastapi',
    'sanic': 'sanic',
    'aiohttp.web': 'aiohttp',
    'quart': 'quart',
    
    # Async & Concurrency
    'asyncio': None,  # Core module
    'concurrent.futures': None,  # Core module
    'threading': None,  # Core module
    'multiprocessing': None,  # Core module
    'gevent': 'gevent',
    'eventlet': 'eventlet',
    'trio': 'trio',
    
    # Data Formats & Serialization
    'json': None,  # Core module
    'pickle': None,  # Core module
    'csv': None,  # Core module
    'xml': None,  # Core module
    'xml.etree.ElementTree': None,  # Core module
    'bs4': 'beautifulsoup4',
    'beautifulsoup4': 'beautifulsoup4',
    'lxml': 'lxml',
    'html.parser': None,  # Core module
    
    # Date & Time
    'datetime': None,  # Core module
    'dateutil': 'python-dateutil',
    'arrow': 'arrow',
    'pendulum': 'pendulum',
    'pytz': 'pytz',
    'timezone': None,  # Core module in Python 3.9+
    
    # File & OS Operations
    'os': None,  # Core module
    'sys': None,  # Core module
    'pathlib': None,  # Core module
    'shutil': None,  # Core module
    'glob': None,  # Core module
    'tempfile': None,  # Core module
    'zipfile': None,  # Core module
    'tarfile': None,  # Core module
    'io': None,  # Core module
    
    # Security & Cryptography
    'hashlib': None,  # Core module
    'hmac': None,  # Core module
    'secrets': None,  # Core module
    'cryptography': 'cryptography',
    'pycryptodome': 'pycryptodome',
    'bcrypt': 'bcrypt',
    'jwt': 'PyJWT',
    'pyjwt': 'PyJWT',
    
    # Image Processing
    'pillow': 'Pillow',
    'PIL': 'Pillow',
    'Image': 'Pillow',
    'cv2': 'opencv-python',
    'opencv': 'opencv-python',
    'opencv_python': 'opencv-python',
    
    # Audio Processing
    'pydub': 'pydub',
    'wave': None,  # Core module
    'audioop': None,  # Core module
    
    # Video Processing
    'moviepy': 'moviepy',
    
    # Machine Learning & AI
    'tensorflow': 'tensorflow',
    'torch': 'torch',
    'pytorch': 'torch',
    'keras': 'keras',
    'sklearn': 'scikit-learn',
    'scikit_learn': 'scikit-learn',
    
    # Natural Language Processing
    'nltk': 'nltk',
    'spacy': 'spacy',
    'transformers': 'transformers',
    
    # Testing
    'pytest': 'pytest',
    'unittest': None,  # Core module
    'mock': 'mock',  # Python 3.3+ has unittest.mock
    'unittest.mock': None,  # Core module in Python 3.3+
    
    # Logging & Monitoring
    'logging': None,  # Core module
    'loguru': 'loguru',
    'structlog': 'structlog',
    'sentry_sdk': 'sentry-sdk',
    
    # Process Management
    'subprocess': None,  # Core module
    'psutil': 'psutil',
    'multiprocessing': None,  # Core module
    
    # Regular Expressions
    're': None,  # Core module
    'regex': 'regex',
    
    # Mathematics
    'math': None,  # Core module
    'random': None,  # Core module
    'statistics': None,  # Core module
    'decimal': None,  # Core module
    'fractions': None,  # Core module
    
    # Network & Sockets
    'socket': None,  # Core module
    'ssl': None,  # Core module
    'socketserver': None,  # Core module
    
    # Email
    'smtplib': None,  # Core module
    'email': None,  # Core module
    
    # Compression
    'gzip': None,  # Core module
    'bz2': None,  # Core module
    'lzma': None,  # Core module
    'zlib': None,  # Core module
    
    # Command Line
    'argparse': None,  # Core module
    'click': 'click',
    'typer': 'typer',
    'fire': 'fire',
    
    # Caching
    'cachetools': 'cachetools',
    'diskcache': 'diskcache',
    
    # Job Scheduling
    'schedule': 'schedule',
    'apscheduler': 'apscheduler',
    'celery': 'celery',
    
    # PDF Processing
    'pyPDF2': 'PyPDF2',
    'reportlab': 'reportlab',
    
    # Excel Processing
    'openpyxl': 'openpyxl',
    'xlrd': 'xlrd',
    'xlwt': 'xlwt',
    
    # Web Scraping
    'scrapy': 'scrapy',
    'selenium': 'selenium',
    'playwright': 'playwright',
    
    # GUI
    'tkinter': None,  # Core module
    'pyqt5': 'PyQt5',
    'pyside2': 'PySide2',
    
    # Game Development
    'pygame': 'pygame',
    
    # System Info
    'platform': None,  # Core module
    'atexit': None,  # Core module
    'signal': None,  # Core module
    
    # Type Hints & Validation
    'typing': None,  # Core module
    'pydantic': 'pydantic',
    'marshmallow': 'marshmallow',
    
    # API Clients
    'googleapiclient': 'google-api-python-client',
    'boto3': 'boto3',
    'dropbox': 'dropbox',
    'twilio': 'twilio',
    
    # Utilities
    'collections': None,  # Core module
    'itertools': None,  # Core module
    'functools': None,  # Core module
    'operator': None,  # Core module
    'inspect': None,  # Core module
    'pdb': None,  # Core module
    'time': None,  # Core module
    'calendar': None,  # Core module
    'locale': None,  # Core module
    'gettext': None,  # Core module
    'codecs': None,  # Core module
    'unicodedata': None,  # Core module
    'string': None,  # Core module
    'textwrap': None,  # Core module
    'difflib': None,  # Core module
    'heapq': None,  # Core module
    'bisect': None,  # Core module
    'array': None,  # Core module
    'weakref': None,  # Core module
    'copy': None,  # Core module
    'pprint': None,  # Core module
    'reprlib': None,  # Core module
    'enum': None,  # Core module
    'types': None,  # Core module
    'dataclasses': None,  # Core module in Python 3.7+
    
    # Version Specific
    'builtins': None,  # Core module
    '__future__': None,  # Core module
    'importlib': None,  # Core module
    'imp': None,  # Core module (deprecated)
    'pkgutil': None,  # Core module
    'modulefinder': None,  # Core module
    'runpy': None,  # Core module
    'sysconfig': None,  # Core module
    
    # Windows Specific
    'msvcrt': None,  # Core module (Windows)
    'winreg': None,  # Core module (Windows)
    
    # Unix Specific
    'grp': None,  # Core module (Unix)
    'pwd': None,  # Core module (Unix)
    'resource': None,  # Core module (Unix)
    
    # Special
    'contextlib': None,  # Core module
    'abc': None,  # Core module
    'atexit': None,  # Core module
    'traceback': None,  # Core module
    '__main__': None,  # Core module
    'warnings': None,  # Core module
    'ast': None,  # Core module
    'symtable': None,  # Core module
    'symbol': None,  # Core module
    'token': None,  # Core module
    'keyword': None,  # Core module
    'tokenize': None,  # Core module
    'tabnanny': None,  # Core module
    'py_compile': None,  # Core module
    'compileall': None,  # Core module
    'dis': None,  # Core module
    'pickletools': None,  # Core module
    
    # Development Tools
    'doctest': None,  # Core module
    'unittest': None,  # Core module
    'test': None,  # Core module
    'profile': None,  # Core module
    'cProfile': None,  # Core module
    'pstats': None,  # Core module
    'timeit': None,  # Core module
    'trace': None,  # Core module
    
    # Internet Protocols
    'cgi': None,  # Core module
    'cgitb': None,  # Core module
    'wsgiref': None,  # Core module
    'urllib.parse': None,  # Core module
    'urllib.error': None,  # Core module
    'urllib.request': None,  # Core module
    'urllib.response': None,  # Core module
    'urllib.robotparser': None,  # Core module
    'http': None,  # Core module
    'ftplib': None,  # Core module
    'poplib': None,  # Core module
    'imaplib': None,  # Core module
    'nntplib': None,  # Core module
    'smtpd': None,  # Core module
    'telnetlib': None,  # Core module
    'uuid': None,  # Core module
    'socketserver': None,  # Core module
    'http.server': None,  # Core module
    'http.cookies': None,  # Core module
    'http.cookiejar': None,  # Core module
    
    # Multimedia
    'audioop': None,  # Core module
    'aifc': None,  # Core module
    'sunau': None,  # Core module
    'wave': None,  # Core module
    'chunk': None,  # Core module
    'colorsys': None,  # Core module
    'imghdr': None,  # Core module
    'sndhdr': None,  # Core module
    'ossaudiodev': None,  # Core module (Unix)
    
    # Internationalization
    'gettext': None,  # Core module
    'locale': None,  # Core module
    
    # Program Frameworks
    'cmd': None,  # Core module
    'shlex': None,  # Core module
    'tkinter': None,  # Core module
    'turtle': None,  # Core module
    
    # Debugging
    'bdb': None,  # Core module
    'faulthandler': None,  # Core module
    'pdb': None,  # Core module
    
    # Software Packaging
    'distutils': None,  # Core module (deprecated)
    'ensurepip': None,  # Core module
    'venv': None,  # Core module
    'zipapp': None,  # Core module
    
    # Python Runtime Services
    'sys': None,  # Core module
    'builtins': None,  # Core module
    '__main__': None,  # Core module
    'warnings': None,  # Core module
    'dataclasses': None,  # Core module in Python 3.7+
    'contextlib': None,  # Core module
    'abc': None,  # Core module
    'atexit': None,  # Core module
    'traceback': None,  # Core module
    
    # Custom Modules (for common typos or variations)
    'request': 'requests',  # Common typo
    'requsts': 'requests',  # Common typo
    'telegrambot': 'python-telegram-bot',  # Common variation
    'telegram_bot': 'python-telegram-bot',  # Common variation
    'telegrambotapi': 'python-telegram-bot',  # Common variation
    'telegram.bot': 'python-telegram-bot',  # Common variation
    'telegram.ext': 'python-telegram-bot',  # Common variation
    'pythontelegrambot': 'python-telegram-bot',  # Common variation
    'ptb': 'python-telegram-bot',  # Abbreviation
    'python_telegrambot': 'python-telegram-bot',  # Common variation
    'env': 'python-dotenv',  # Common shorthand
    'pythonenv': 'python-dotenv',  # Common variation
    'dot_env': 'python-dotenv',  # Common variation
    'python_dot_env': 'python-dotenv',  # Common variation
    
    # Additional common modules
    'colorama': 'colorama',
    'termcolor': 'termcolor',
    'rich': 'rich',
    'tqdm': 'tqdm',
    'progressbar2': 'progressbar2',
    'humanize': 'humanize',
    'emoji': 'emoji',
    'pyfiglet': 'pyfiglet',
    'art': 'art',
    
    # Message queues
    'pika': 'pika',
    'kombu': 'kombu',
    
    # GraphQL
    'graphene': 'graphene',
    'ariadne': 'ariadne',
    
    # Graph databases
    'neo4j': 'neo4j',
    'py2neo': 'py2neo',
    
    # Search engines
    'whoosh': 'whoosh',
    'elasticsearch': 'elasticsearch',
    'pysolr': 'pysolr',
    
    # GIS
    'geopy': 'geopy',
    'shapely': 'shapely',
    
    # Finance
    'yfinance': 'yfinance',
    'pandas_datareader': 'pandas-datareader',
    
    # WebSocket
    'websockets': 'websockets',
    'ws4py': 'ws4py',
    
    # Serial communication
    'pyserial': 'pyserial',
    
    # Bluetooth
    'pybluez': 'pybluez',
    
    # SSH
    'paramiko': 'paramiko',
    'fabric': 'fabric',
    
    # DNS
    'dnspython': 'dnspython',
    
    # Voice recognition
    'speech_recognition': 'SpeechRecognition',
    'pyaudio': 'PyAudio',
    
    # Screen capture
    'pyautogui': 'pyautogui',
    'mss': 'mss',
    
    # Hardware
    'RPi.GPIO': 'RPi.GPIO',  # Raspberry Pi
    'gpiozero': 'gpiozero',  # Raspberry Pi
    'adafruit_blinka': 'adafruit-blinka',  # CircuitPython
    'board': 'adafruit-blinka',  # CircuitPython
    
    # Cloud services
    'boto': 'boto',
    'azure': 'azure',
    'google.cloud': 'google-cloud',
    
    # Blockchain
    'web3': 'web3',
    'bitcoin': 'python-bitcoinlib',
    
    # Bioinformatics
    'biopython': 'biopython',
    
    # Astronomy
    'astropy': 'astropy',
    
    # Chemistry
    'rdkit': 'rdkit',
    
    # Physics
    'sympy': 'sympy',
    
    # Statistics
    'statsmodels': 'statsmodels',
    
    # Computer Vision (additional)
    'face_recognition': 'face-recognition',
    'dlib': 'dlib',
    
    # Audio (additional)
    'librosa': 'librosa',
    'pydub': 'pydub',
    
    # Video (additional)
    'imageio': 'imageio',
    'imageio_ffmpeg': 'imageio-ffmpeg',
    
    # Data visualization (additional)
    'bokeh': 'bokeh',
    'altair': 'altair',
    'plotnine': 'plotnine',
    
    # Geospatial
    'geopandas': 'geopandas',
    'folium': 'folium',
    'basemap': 'basemap',
    
    # Time series
    'prophet': 'prophet',
    'arch': 'arch',
    
    # Reinforcement learning
    'gym': 'gym',
    'stable_baselines3': 'stable-baselines3',
    
    # Deep learning (additional)
    'torchvision': 'torchvision',
    'torchaudio': 'torchaudio',
    'fastai': 'fastai',
    'lightning': 'pytorch-lightning',
    
    # Optimization
    'pulp': 'pulp',
    'ortools': 'ortools',
    
    # Robotics
    'roslibpy': 'roslibpy',
    
    # IoT
    'paho.mqtt': 'paho-mqtt',
    'pymqtt': 'paho-mqtt',
    
    # Augmented Reality
    'cv2.aruco': 'opencv-contrib-python',
    
    # Virtual Reality
    'openvr': 'openvr',
    
    # Game AI
    'pettingzoo': 'pettingzoo',
    
    # Music
    'music21': 'music21',
    'mingus': 'mingus',
    
    # Linguistics
    'textblob': 'textblob',
    'pattern': 'pattern',
    
    # OCR
    'pytesseract': 'pytesseract',
    'easyocr': 'easyocr',
    
    # PDF (additional)
    'fitz': 'PyMuPDF',
    'pymupdf': 'PyMuPDF',
    
    # Excel (additional)
    'pandas.io.excel': 'pandas',  # Part of pandas
    'xlsxwriter': 'XlsxWriter',
    
    # Word documents
    'docx': 'python-docx',
    
    # PowerPoint
    'pptx': 'python-pptx',
    
    # Email (additional)
    'yagmail': 'yagmail',
    
    # SMS
    'textmagic': 'textmagic',
    
    # Voice calling
    'plivo': 'plivo',
    
    # Video calling
    'opentok': 'opentok',
    
    # Social media APIs
    'tweepy': 'tweepy',
    'facebook_sdk': 'facebook-sdk',
    'instagram_private_api': 'instagram-private-api',
    
    # Payment gateways (additional)
    'razorpay': 'razorpay',
    'stripe': 'stripe',
    
    # E-commerce platforms
    'shopify': 'shopify',
    'woocommerce': 'woocommerce',
    
    # CMS
    'wordpress_xmlrpc': 'python-wordpress-xmlrpc',
    
    # Project management
    'jira': 'jira',
    
    # Customer support
    'zendesk': 'zendesk',
    
    # Accounting
    'quickbooks': 'quickbooks',
    
    # HR
    'bamboohr': 'bamboohr',
    
    # CRM (additional)
    'salesforce': 'simple-salesforce',
    'hubspot': 'hubspot',
    
    # Marketing automation
    'mailchimp': 'mailchimp',
    'sendgrid': 'sendgrid',
    
    # Analytics (additional)
    'google.analytics': 'google-analytics',
    'mixpanel': 'mixpanel',
    
    # A/B testing
    'optimizely': 'optimizely',
    
    # User feedback
    'uservoice': 'uservoice',
    
    # Surveys
    'surveygizmo': 'surveygizmo',
    
    # Help desk
    'freshdesk': 'freshdesk',
    'zendesk': 'zendesk',
    
    # Live chat
    'intercom': 'intercom',
    'drift': 'drift',
    
    # Monitoring (additional)
    'newrelic': 'newrelic',
    'datadog': 'datadog',
    
    # Error tracking (additional)
    'rollbar': 'rollbar',
    'bugsnag': 'bugsnag',
    
    # Performance monitoring
    'scout': 'scout',
    
    # Logging (additional)
    'logstash': 'logstash',
    'fluent': 'fluent',
    
    # APM
    'appdynamics': 'appdynamics',
    
    # Infrastructure as code
    'boto3': 'boto3',
    'terraform': 'terraform',
    
    # Containerization
    'docker': 'docker',
    'kubernetes': 'kubernetes',
    
    # Serverless
    'aws_lambda': 'aws-lambda',
    'serverless': 'serverless',
    
    # CI/CD
    'jenkins': 'jenkins',
    'travis': 'travis',
    
    # Version control
    'git': 'gitpython',
    'github': 'PyGithub',
    
    # Documentation
    'sphinx': 'sphinx',
    'mkdocs': 'mkdocs',
    
    # Code quality
    'flake8': 'flake8',
    'pylint': 'pylint',
    'black': 'black',
    'isort': 'isort',
    
    # Dependency management
    'pip': None,  # Usually comes with Python
    'pipenv': 'pipenv',
    'poetry': 'poetry',
    
    # Virtual environments
    'virtualenv': 'virtualenv',
    'venv': None,  # Core module
    
    # Package building
    'setuptools': 'setuptools',
    'wheel': 'wheel',
    'twine': 'twine',
    
    # Testing (additional)
    'tox': 'tox',
    'nose': 'nose',
    'coverage': 'coverage',
    
    # Performance testing
    'locust': 'locust',
    'jmeter': 'jmeter',
    
    # Security testing
    'bandit': 'bandit',
    'safety': 'safety',
    
    # Code analysis
    'radon': 'radon',
    'vulture': 'vulture',
    
    # Profiling
    'line_profiler': 'line-profiler',
    'memory_profiler': 'memory-profiler',
    
    # Debugging (additional)
    'ipdb': 'ipdb',
    'pudb': 'pudb',
    
    # REPL enhancements
    'ipython': 'ipython',
    'ptpython': 'ptpython',
    'bpython': 'bpython',
    
    # Jupyter
    'jupyter': 'jupyter',
    'notebook': 'notebook',
    'jupyterlab': 'jupyterlab',
    
    # Data science notebooks
    'voila': 'voila',
    'streamlit': 'streamlit',
    'dash': 'dash',
    
    # Dashboarding
    'panel': 'panel',
    'holoviews': 'holoviews',
    
    # Business intelligence
    'superset': 'apache-superset',
    'redash': 'redash',
    
    # ETL
    'airflow': 'apache-airflow',
    'luigi': 'luigi',
    'prefect': 'prefect',
    
    # Data validation
    'great_expectations': 'great-expectations',
    'pandera': 'pandera',
    
    # Feature stores
    'feast': 'feast',
    
    # Model deployment
    'mlflow': 'mlflow',
    'bentoml': 'bentoml',
    
    # Model serving
    'seldon': 'seldon',
    'kserve': 'kserve',
    
    # Experiment tracking
    'wandb': 'wandb',
    'comet': 'comet',
    
    # Hyperparameter optimization
    'optuna': 'optuna',
    'hyperopt': 'hyperopt',
    
    # AutoML
    'autosklearn': 'auto-sklearn',
    'tpot': 'tpot',
    
    # Explainable AI
    'shap': 'shap',
    'lime': 'lime',
    
    # Fairness & ethics
    'aif360': 'aif360',
    'fairlearn': 'fairlearn',
    
    # Privacy
    'diffprivlib': 'diffprivlib',
    
    # Federated learning
    'flower': 'flwr',
    
    # Quantum computing
    'qiskit': 'qiskit',
    'cirq': 'cirq',
    
    # Bioinformatics (additional)
    'pysam': 'pysam',
    'bioconductor': 'bioconductor',
    
    # Astronomy (additional)
    'sunpy': 'sunpy',
    'astroquery': 'astroquery',
    
    # Chemistry (additional)
    'openbabel': 'openbabel',
    'rdkit.Chem': 'rdkit',
    
    # Physics (additional)
    'quantum': 'quantum',
    'particle': 'particle',
    
    # Mathematics (additional)
    'mpmath': 'mpmath',
    'gmpy2': 'gmpy2',
    
    # Signal processing
    'scipy.signal': 'scipy',
    'pywavelets': 'pywavelets',
    
    # Control systems
    'control': 'control',
    
    # Robotics (additional)
    'pybullet': 'pybullet',
    'gymnasium': 'gymnasium',
    
    # Drone control
    'dronekit': 'dronekit',
    
    # Self-driving cars
    'carla': 'carla',
    
    # Natural language (additional)
    'gensim': 'gensim',
    'fasttext': 'fasttext',
    'word2vec': 'gensim',  # Usually through gensim
    
    # Speech (additional)
    'whisper': 'openai-whisper',
    'vosk': 'vosk',
    
    # Music (additional)
    'pretty_midi': 'pretty-midi',
    'mido': 'mido',
    
    # Art & design
    'cairosvg': 'cairosvg',
    'svglib': 'svglib',
    
    # 3D graphics
    'pyopengl': 'PyOpenGL',
    'vpython': 'vpython',
    
    # Game development (additional)
    'arcade': 'arcade',
    'pyglet': 'pyglet',
    
    # Simulation
    'simpy': 'simpy',
    
    # Network simulation
    'mininet': 'mininet',
    
    # Financial markets simulation
    'zipline': 'zipline',
    
    # Epidemiology
    'epipy': 'epipy',
    
    # Climate science
    'xarray': 'xarray',
    'netCDF4': 'netCDF4',
    
    # Hydrology
    'pysheds': 'pysheds',
    
    # Agriculture
    'cropmodel': 'cropmodel',
    
    # Energy
    'pandas_power': 'pandas-power',
    
    # Transportation
    'osmnx': 'osmnx',
    'networkx': 'networkx',
    
    # Urban planning
    'urbanaccess': 'urbanaccess',
    
    # Public health
    'epimodel': 'epimodel',
    
    # Economics
    'pygameconomics': 'pygameconomics',
    
    # Sociology
    'sociology': 'sociology',
    
    # Psychology
    'psychopy': 'psychopy',
    
    # Neuroscience
    'nipy': 'nipy',
    'brainflow': 'brainflow',
    
    # Medical imaging
    'pydicom': 'pydicom',
    'simpleitk': 'SimpleITK',
    
    # Genomics
    'pygenomics': 'pygenomics',
    
    # Proteomics
    'pyteomics': 'pyteomics',
    
    # Metabolomics
    'pymetabo': 'pymetabo',
    
    # Phylogenetics
    'biopython.Phylo': 'biopython',
    
    # Ecology
    'pyecology': 'pyecology',
    
    # Zoology
    'animaltracker': 'animaltracker',
    
    # Botany
    'plantcv': 'plantcv',
    
    # Microbiology
    'pymicro': 'pymicro',
    
    # Virology
    'pyvirus': 'pyvirus',
    
    # Bacteriology
    'pybacteria': 'pybacteria',
    
    # Mycology
    'pymycology': 'pymycology',
    
    # Entomology
    'pyentomology': 'pyentomology',
    
    # Ornithology
    'pyornithology': 'pyornithology',
    
    # Ichthyology
    'pyichthyology': 'pyichthyology',
    
    # Herpetology
    'pyherpetology': 'pyherpetology',
    
    # Mammalogy
    'pymammalogy': 'pymammalogy',
    
    # Paleontology
    'pypaleontology': 'pypaleontology',
    
    # Geology
    'pygeology': 'pygeology',
    
    # Meteorology
    'pymeteorology': 'pymeteorology',
    
    # Oceanography
    'pyoceanography': 'pyoceanography',
    
    # Seismology
    'pyseismology': 'pyseismology',
    
    # Volcanology
    'pyvolcanology': 'pyvolcanology',
    
    # Astronomy (even more)
    'astroplan': 'astroplan',
    'astroscrappy': 'astroscrappy',
    
    # Space weather
    'spaceweather': 'spaceweather',
    
    # Satellite data
    'satpy': 'satpy',
    
    # Remote sensing
    'rasterio': 'rasterio',
    'georaster': 'georaster',
    
    # GIS (additional)
    'pyproj': 'pyproj',
    'fiona': 'fiona',
    
    # Cartography
    'cartopy': 'cartopy',
    
    # Surveying
    'pysurvey': 'pysurvey',
    
    # Archaeology
    'pyarchaeology': 'pyarchaeology',
    
    # Anthropology
    'pyanthropology': 'pyanthropology',
    
    # History
    'pyhistory': 'pyhistory',
    
    # Linguistics (additional)
    'polyglot': 'polyglot',
    'langid': 'langid',
    
    # Literature
    'pyliterature': 'pyliterature',
    
    # Poetry analysis
    'pypoetry': 'pypoetry',
    
    # Music theory
    'musicanalysis': 'musicanalysis',
    
    # Art history
    'pyarthistory': 'pyarthistory',
    
    # Film studies
    'pyfilmstudies': 'pyfilmstudies',
    
    # Theater
    'pytheater': 'pytheater',
    
    # Dance
    'pydance': 'pydance',
    
    # Sports analytics
    'sportsipy': 'sportsipy',
    'nba_api': 'nba_api',
    
    # Esports
    'pytesports': 'pytesports',
    
    # Gaming (additional)
    'steam': 'steam',
    'discord.py': 'discord.py',
    
    # Social media analysis
    'socialmedia': 'socialmedia',
    
    # Web scraping (additional)
    'mechanicalsoup': 'mechanicalsoup',
    'newspaper3k': 'newspaper3k',
    
    # Browser automation (additional)
    'splinter': 'splinter',
    
    # Headless browsers
    'pyppeteer': 'pyppeteer',
    
    # API testing
    'tavern': 'tavern',
    
    # Load testing
    'gatling': 'gatling',
    
    # Chaos engineering
    'chaostoolkit': 'chaostoolkit',
    
    # Disaster recovery
    'drpy': 'drpy',
    
    # Backup
    'pybackup': 'pybackup',
    
    # Encryption (additional)
    'gnupg': 'python-gnupg',
    'keyring': 'keyring',
    
    # Authentication
    'authlib': 'authlib',
    'oauthlib': 'oauthlib',
    
    # Authorization
    'casbin': 'casbin',
    
    # Blockchain (additional)
    'eth_account': 'eth-account',
    'web3.py': 'web3',
    
    # Cryptocurrency
    'ccxt': 'ccxt',
    'coinbase': 'coinbase',
    
    # NFTs
    'web3.storage': 'web3.storage',
    
    # DeFi
    'defipy': 'defipy',
    
    # Smart contracts
    'vyper': 'vyper',
    
    # DAOs
    'dao.py': 'dao.py',
    
    # Web3 social
    'lenspy': 'lenspy',
    
    # Metaverse
    'metaversepy': 'metaversepy',
    
    # AR/VR (additional)
    'openxr': 'openxr',
    
    # IoT (additional)
    'adafruit_io': 'adafruit-io',
    'thingspeak': 'thingspeak',
    
    # Home automation
    'homeassistant': 'homeassistant',
    'pyHS100': 'pyHS100',
    
    # Robotics kits
    'ev3dev2': 'ev3dev2',
    'micropython': 'micropython',
    
    # Drones (additional)
    'tello': 'tello',
    'parrot': 'parrot',
    
    # Self-driving (additional)
    'donkeycar': 'donkeycar',
    
    # Computer vision for robotics
    'apriltag': 'apriltag',
    
    # Sensor data
    'sense_hat': 'sense-hat',
    'picamera': 'picamera',
    
    # Wearables
    'pywearables': 'pywearables',
    
    # Health monitoring
    'pyhealth': 'pyhealth',
    
    # Fitness tracking
    'fitbit': 'fitbit',
    
    # Medical devices
    'pymeddevice': 'pymeddevice',
    
    # Laboratory equipment
    'pylabware': 'pylabware',
    
    # Scientific instruments
    'pyinstrument': 'pyinstrument',
    
    # Test equipment
    'pyvxi11': 'pyvxi11',
    
    # Industrial automation
    'pycomm': 'pycomm',
    'pymodbus': 'pymodbus',
    
    # PLC programming
    'pyads': 'pyads',
    
    # SCADA
    'pyscada': 'pyscada',
    
    # Building automation
    'bacpypes': 'bacpypes',
    
    # Energy management
    'pyenergy': 'pyenergy',
    
    # Smart grid
    'pysmartgrid': 'pysmartgrid',
    
    # Renewable energy
    'pyrenewable': 'pyrenewable',
    
    # Electric vehicles
    'pyev': 'pyev',
    
    # Charging stations
    'pycharge': 'pycharge',
    
    # Traffic management
    'pytraffic': 'pytraffic',
    
    # Public transportation
    'pytransit': 'pytransit',
    
    # Ride sharing
    'pyride': 'pyride',
    
    # Food delivery
    'pyfood': 'pyfood',
    
    # E-commerce logistics
    'pylogistics': 'pylogistics',
    
    # Supply chain
    'pysupplychain': 'pysupplychain',
    
    # Inventory management
    'pyinventory': 'pyinventory',
    
    # Warehouse management
    'pywarehouse': 'pywarehouse',
    
    # Retail analytics
    'pyretail': 'pyretail',
    
    # Point of sale
    'pypos': 'pypos',
    
    # Restaurant management
    'pyrestaurant': 'pyrestaurant',
    
    # Hotel management
    'pyhotel': 'pyhotel',
    
    # Travel booking
    'pytravel': 'pytravel',
    
    # Flight tracking
    'pyflight': 'pyflight',
    
    # Weather forecasting
    'pyweather': 'pyweather',
    
    # Disaster预警
    'pydisaster': 'pydisaster',
    
    # Emergency response
    'pyemergency': 'pyemergency',
    
    # Search and rescue
    'pysearchrescue': 'pysearchrescue',
    
    # Firefighting
    'pyfire': 'pyfire',
    
    # Police work
    'pypolice': 'pypolice',
    
    # Military applications
    'pymilitary': 'pymilitary',
    
    # Cybersecurity
    'scapy': 'scapy',
    'nmap': 'python-nmap',
    
    # Digital forensics
    'pyforensics': 'pyforensics',
    
    # Malware analysis
    'pymalware': 'pymalware',
    
    # Threat intelligence
    'pyti': 'pyti',
    
    # Incident response
    'pyir': 'pyir',
    
    # Vulnerability scanning
    'pyvulnscan': 'pyvulnscan',
    
    # Penetration testing
    'pypentest': 'pypentest',
    
    # Red teaming
    'pyredteam': 'pyredteam',
    
    # Blue teaming
    'pyblueteam': 'pyblueteam',
    
    # Purple teaming
    'pypurpleteam': 'pypurpleteam',
    
    # Security orchestration
    'pysoar': 'pysoar',
    
    # Threat hunting
    'pythreathunt': 'pythreathunt',
    
    # Fraud detection
    'pyfraud': 'pyfraud',
    
    # Risk management
    'pyrism': 'pyrism',
    
    # Compliance
    'pycompliance': 'pycompliance',
    
    # Governance
    'pygovernance': 'pygovernance',
    
    # Audit
    'pyaudit': 'pyaudit',
    
    # Legal tech
    'pylegal': 'pylegal',
    
    # Contract analysis
    'pycontract': 'pycontract',
    
    # Patent analysis
    'pypatent': 'pypatent',
    
    # Court case analysis
    'pycourt': 'pycourt',
    
    # Law enforcement
    'pylawenforcement': 'pylawenforcement',
    
    # Judiciary
    'pyjudiciary': 'pyjudiciary',
    
    # Legislation
    'pylegislation': 'pylegislation',
    
    # Public policy
    'pypublicpolicy': 'pypublicpolicy',
    
    # Government services
    'pygovernment': 'pygovernment',
    
    # Voting systems
    'pyvoting': 'pyvoting',
    
    # Census data
    'pycensus': 'pycensus',
    
    # Tax calculation
    'pytax': 'pytax',
    
    # Social security
    'pysocialsecurity': 'pysocialsecurity',
    
    # Healthcare administration
    'pyhealthadmin': 'pyhealthadmin',
    
    # Medical billing
    'pymedicalbilling': 'pymedicalbilling',
    
    # Insurance
    'pyinsurance': 'pyinsurance',
    
    # Claims processing
    'pyclaims': 'pyclaims',
    
    # Underwriting
    'pyunderwriting': 'pyunderwriting',
    
    # Actuarial science
    'pyactuarial': 'pyactuarial',
    
    # Risk assessment
    'pyrismassessment': 'pyrismassessment',
    
    # Investment banking
    'pyinvestmentbanking': 'pyinvestmentbanking',
    
    # Asset management
    'pyassetmanagement': 'pyassetmanagement',
    
    # Wealth management
    'pywealthmanagement': 'pywealthmanagement',
    
    # Private equity
    'pype': 'pype',
    
    # Venture capital
    'pyvc': 'pyvc',
    
    # Hedge funds
    'pyhedgefund': 'pyhedgefund',
    
    # Trading (additional)
    'backtrader': 'backtrader',
    'zipline': 'zipline',
    
    # Algorithmic trading
    'pyalgotrade': 'pyalgotrade',
    
    # Quantitative finance
    'quantlib': 'QuantLib',
    
    # Options pricing
    'pyoption': 'pyoption',
    
    # Derivatives
    'pyderivatives': 'pyderivatives',
    
    # Fixed income
    'pyfixedincome': 'pyfixedincome',
    
    # Forex
    'pyforex': 'pyforex',
    
    # Commodities
    'pycommodities': 'pycommodities',
    
    # Real estate
    'pyrealestate': 'pyrealestate',
    
    # Property management
    'pyproperty': 'pyproperty',
    
    # Construction
    'pyconstruction': 'pyconstruction',
    
    # Architecture
    'pyarchitecture': 'pyarchitecture',
    
    # Interior design
    'pyinteriordesign': 'pyinteriordesign',
    
    # Landscape architecture
    'pylandscape': 'pylandscape',
    
    # Urban design
    'pyurbandesign': 'pyurbandesign',
    
    # Regional planning
    'pyregionalplanning': 'pyregionalplanning',
    
    # Environmental planning
    'pyenvironmentalplanning': 'pyenvironmentalplanning',
    
    # Sustainability
    'pysustainability': 'pysustainability',
    
    # Climate action
    'pyclimateaction': 'pyclimateaction',
    
    # Carbon accounting
    'pycarbon': 'pycarbon',
    
    # ESG (Environmental, Social, Governance)
    'pyesg': 'pyesg',
    
    # Corporate social responsibility
    'pycsr': 'pycsr',
    
    # Philanthropy
    'pyphilanthropy': 'pyphilanthropy',
    
    # Nonprofit management
    'pynonprofit': 'pynonprofit',
    
    # Fundraising
    'pyfundraising': 'pyfundraising',
    
    # Grant writing
    'pygrantwriting': 'pygrantwriting',
    
    # Volunteer management
    'pyvolunteer': 'pyvolunteer',
    
    # Community organizing
    'pycommunity': 'pycommunity',
    
    # Social work
    'pysocialwork': 'pysocialwork',
    
    # Counseling
    'pycounseling': 'pycounseling',
    
    # Therapy
    'pytherapy': 'pytherapy',
    
    # Mental health
    'pymentalhealth': 'pymentalhealth',
    
    # Wellness
    'pywellness': 'pywellness',
    
    # Meditation
    'pymeditation': 'pymeditation',
    
    # Yoga
    'pyyoga': 'pyyoga',
    
    # Fitness (additional)
    'pyfitness': 'pyfitness',
    
    # Nutrition
    'pynutrition': 'pynutrition',
    
    # Diet planning
    'pydietplanning': 'pydietplanning',
    
    # Recipe management
    'pyrecipe': 'pyrecipe',
    
    # Cooking
    'pycooking': 'pycooking',
    
    # Baking
    'pybaking': 'pybaking',
    
    # Bartending
    'pybartending': 'pybartending',
    
    # Mixology
    'pymixology': 'pymixology',
    
    # Wine tasting
    'pywine': 'pywine',
    
    # Coffee brewing
    'pycoffee': 'pycoffee',
    
    # Tea tasting
    'pytea': 'pytea',
    
    # Chocolate making
    'pychocolate': 'pychocolate',
    
    # Cheese making
    'pycheese': 'pycheese',
    
    # Brewing
    'pybrewing': 'pybrewing',
    
    # Distilling
    'pydistilling': 'pydistilling',
    
    # Fermentation
    'pyfermentation': 'pyfermentation',
    
    # Gardening
    'pygardening': 'pygardening',
    
    # Farming
    'pyfarming': 'pyfarming',
    
    # Aquaculture
    'pyaquaculture': 'pyaquaculture',
    
    # Hydroponics
    'pyhydroponics': 'pyhydroponics',
    
    # Aquaponics
    'pyaquaponics': 'pyaquaponics',
    
    # Permaculture
    'pypermaculture': 'pypermaculture',
    
    # Beekeeping
    'pybeekeeping': 'pybeekeeping',
    
    # Chicken keeping
    'pychicken': 'pychicken',
    
    # Goat keeping
    'pygoat': 'pygoat',
    
    # Sheep keeping
    'pysheep': 'pysheep',
    
    # Cow keeping
    'pycow': 'pycow',
    
    # Horse keeping
    'pyhorse': 'pyhorse',
    
    # Dog training
    'pydog': 'pydog',
    
    # Cat care
    'pycat': 'pycat',
    
    # Bird watching
    'pybirdwatching': 'pybirdwatching',
    
    # Wildlife photography
    'pywildlifephotography': 'pywildlifephotography',
    
    # Nature conservation
    'pynatureconservation': 'pynatureconservation',
    
    # Environmental protection
    'pyenvironmentalprotection': 'pyenvironmentalprotection',
    
    # Pollution control
    'pypollutioncontrol': 'pypollutioncontrol',
    
    # Waste management
    'pywastemanagement': 'pywastemanagement',
    
    # Recycling
    'pyrecycling': 'pyrecycling',
    
    # Upcycling
    'pyupcycling': 'pyupcycling',
    
    # Circular economy
    'pycirculareconomy': 'pycirculareconomy',
    
    # Green technology
    'pygreentech': 'pygreentech',
    
    # Clean energy
    'pycleanenergy': 'pycleanenergy',
    
    # Solar power
    'pysolar': 'pysolar',
    
    # Wind power
    'pywind': 'pywind',
    
    # Hydro power
    'pyhydro': 'pyhydro',
    
    # Geothermal
    'pygeothermal': 'pygeothermal',
    
    # Biomass
    'pybiomass': 'pybiomass',
    
    # Tidal power
    'pytidal': 'pytidal',
    
    # Wave power
    'pywave': 'pywave',
    
    # Nuclear energy
    'pynuclear': 'pynuclear',
    
    # Energy storage
    'pyenergystorage': 'pyenergystorage',
    
    # Smart homes
    'pysmarthome': 'pysmarthome',
    
    # Home security
    'pyhomesecurity': 'pyhomesecurity',
    
    # Home entertainment
    'pyhomeentertainment': 'pyhomeentertainment',
    
    # Home networking
    'pyhomenetworking': 'pyhomenetworking',
    
    # Home automation (additional)
    'pyhomeautomation': 'pyhomeautomation',
    
    # Smart cities
    'pysmartcity': 'pysmartcity',
    
    # Urban farming
    'pyurbanfarming': 'pyurbanfarming',
    
    # Vertical farming
    'pyverticalfarming': 'pyverticalfarming',
    
    # Rooftop gardening
    'pyrooftopgardening': 'pyrooftopgardening',
    
    # Balcony gardening
    'pybalconygardening': 'pybalconygardening',
    
    # Indoor gardening
    'pyindoorgardening': 'pyindoorgardening',
    
    # Terrarium making
    'pyterrarium': 'pyterrarium',
    
    # Aquarium keeping
    'pyaquarium': 'pyaquarium',
    
    # Vivarium keeping
    'pyvivarium': 'pyvivarium',
    
    # Insect keeping
    'pyinsect': 'pyinsect',
    
    # Reptile keeping
    'pyreptile': 'pyreptile',
    
    # Amphibian keeping
    'pyamphibian': 'pyamphibian',
    
    # Fish keeping
    'pyfish': 'pyfish',
    
    # Coral keeping
    'pycoral': 'pycoral',
    
    # Plant care
    'pyplantcare': 'pyplantcare',
    
    # Bonsai
    'pybonsai': 'pybonsai',
    
    # Succulent care
    'pysucculent': 'pysucculent',
    
    # Orchid care
    'pyorchid': 'pyorchid',
    
    # Rose care
    'pyrose': 'pyrose',
    
    # Herb gardening
    'pyherbgardening': 'pyherbgardening',
    
    # Vegetable gardening
    'pyvegetablegardening': 'pyvegetablegardening',
    
    # Fruit gardening
    'pyfruitgardening': 'pyfruitgardening',
    
    # Mushroom cultivation
    'pymushroom': 'pymushroom',
    
    # Microgreens
    'pymicrogreens': 'pymicrogreens',
    
    # Sprouting
    'pysprouting': 'pysprouting',
    
    # Composting
    'pycomposting': 'pycomposting',
    
    # Vermiculture
    'pyvermiculture': 'pyvermiculture',
    
    # Soil science
    'pysoil': 'pysoil',
    
    # Plant pathology
    'pyplantpathology': 'pyplantpathology',
    
    # Plant breeding
    'pyplantbreeding': 'pyplantbreeding',
    
    # Plant genetics
    'pyplantgenetics': 'pyplantgenetics',
    
    # Plant biotechnology
    'pyplantbiotechnology': 'pyplantbiotechnology',
    
    # Agricultural engineering
    'pyagriculturalengineering': 'pyagriculturalengineering',
    
    # Agricultural economics
    'pyagriculturaleconomics': 'pyagriculturaleconomics',
    
    # Agribusiness
    'pyagribusiness': 'pyagribusiness',
    
    # Food science
    'pyfoodscience': 'pyfoodscience',
    
    # Food technology
    'pyfoodtechnology': 'pyfoodtechnology',
    
    # Food safety
    'pyfoodsafety': 'pyfoodsafety',
    
    # Food preservation
    'pyfoodpreservation': 'pyfoodpreservation',
    
    # Food packaging
    'pyfoodpackaging': 'pyfoodpackaging',
    
    # Food distribution
    'pyfooddistribution': 'pyfooddistribution',
    
    # Food retail
    'pyfoodretail': 'pyfoodretail',
    
    # Food service
    'pyfoodservice': 'pyfoodservice',
    
    # Restaurant management (additional)
    'pyrestaurantmanagement': 'pyrestaurantmanagement',
    
    # Hotel management (additional)
    'pyhotelmanagement': 'pyhotelmanagement',
    
    # Tourism management
    'pytourismmanagement': 'pytourismmanagement',
    
    # Event management
    'pyeventmanagement': 'pyeventmanagement',
    
    # Wedding planning
    'pywedding': 'pywedding',
    
    # Party planning
    'pyparty': 'pyparty',
    
    # Conference planning
    'pyconference': 'pyconference',
    
    # Exhibition planning
    'pyexhibition': 'pyexhibition',
    
    # Trade show planning
    'pytradeshow': 'pytradeshow',
    
    # Festival planning
    'pyfestival': 'pyfestival',
    
    # Concert planning
    'pyconcert': 'pyconcert',
    
    # Theater production
    'pytheaterproduction': 'pytheaterproduction',
    
    # Film production
    'pyfilmproduction': 'pyfilmproduction',
    
    # TV production
    'pytvproduction': 'pytvproduction',
    
    # Radio production
    'pyradioproduction': 'pyradioproduction',
    
    # Podcast production
    'pypodcast': 'pypodcast',
    
    # Video production
    'pyvideoproduction': 'pyvideoproduction',
    
    # Audio production
    'pyaudioproduction': 'pyaudioproduction',
    
    # Music production
    'pymusicproduction': 'pymusicproduction',
    
    # Sound design
    'pysounddesign': 'pysounddesign',
    
    # Game development (additional)
    'pygamedevelopment': 'pygamedevelopment',
    
    # App development
    'pyappdevelopment': 'pyappdevelopment',
    
    # Web development
    'pywebdevelopment': 'pywebdevelopment',
    
    # Software development
    'pysoftwaredevelopment': 'pysoftwaredevelopment',
    
    # DevOps
    'pydevops': 'pydevops',
    
    # Data engineering
    'pydataengineering': 'pydataengineering',
    
    # Machine learning engineering
    'pymachinelearningengineering': 'pymachinelearningengineering',
    
    # AI engineering
    'pyaiengineering': 'pyaiengineering',
    
    # Robotics engineering
    'pyroboticsengineering': 'pyroboticsengineering',
    
    # Electrical engineering
    'pyelectricalengineering': 'pyelectricalengineering',
    
    # Mechanical engineering
    'pymechanicalengineering': 'pymechanicalengineering',
    
    # Civil engineering
    'pycivilengineering': 'pycivilengineering',
    
    # Chemical engineering
    'pychemicalengineering': 'pychemicalengineering',
    
    # Biomedical engineering
    'pybiomedicalengineering': 'pybiomedicalengineering',
    
    # Aerospace engineering
    'pyaerospaceengineering': 'pyaerospaceengineering',
    
    # Automotive engineering
    'pyautomotiveengineering': 'pyautomotiveengineering',
    
    # Marine engineering
    'pymarineengineering': 'pymarineengineering',
    
    # Nuclear engineering
    'pynuclearengineering': 'pynuclearengineering',
    
    # Environmental engineering
    'pyenvironmentalengineering': 'pyenvironmentalengineering',
    
    # Materials engineering
    'pymaterialsengineering': 'pymaterialsengineering',
    
    # Industrial engineering
    'pyindustrialengineering': 'pyindustrialengineering',
    
    # Systems engineering
    'pysystemsengineering': 'pysystemsengineering',
    
    # Network engineering
    'pynetworkengineering': 'pynetworkengineering',
    
    # Software engineering
    'pysoftwareengineering': 'pysoftwareengineering',
    
    # Computer engineering
    'pycomputerengineering': 'pycomputerengineering',
    
    # Telecommunications engineering
    'pytelecommunicationsengineering': 'pytelecommunicationsengineering',
    
    # Optical engineering
    'pyopticalengineering': 'pyopticalengineering',
    
    # Acoustical engineering
    'pyacousticalengineering': 'pyacousticalengineering',
    
    # Geological engineering
    'pygeologicalengineering': 'pygeologicalengineering',
    
    # Mining engineering
    'pyminingengineering': 'pyminingengineering',
    
    # Petroleum engineering
    'pypetroleumengineering': 'pypetroleumengineering',
    
    # Textile engineering
    'pytextileengineering': 'pytextileengineering',
    
    # Agricultural engineering (additional)
    'pyagriculturalengineering': 'pyagriculturalengineering',
    
    # Food engineering
    'pyfoodengineering': 'pyfoodengineering',
    
    # Pharmaceutical engineering
    'pypharmaceuticalengineering': 'pypharmaceuticalengineering',
    
    # Biotechnology engineering
    'pybiotechnologyengineering': 'pybiotechnologyengineering',
    
    # Nanotechnology engineering
    'pynanotechnologyengineering': 'pynanotechnologyengineering',
    
    # Quantum engineering
    'pyquantumengineering': 'pyquantumengineering',
    
    # Energy engineering
    'pyenergyengineering': 'pyenergyengineering',
    
    # Renewable energy engineering
    'pyrenewableenergyengineering': 'pyrenewableenergyengineering',
    
    # Sustainable engineering
    'pysustainableengineering': 'pysustainableengineering',
    
    # Green engineering
    'pygreenengineering': 'pygreenengineering',
    
    # Ecological engineering
    'pyecologicalengineering': 'pyecologicalengineering',
    
    # Climate engineering
    'pyclimateengineering': 'pyclimateengineering',
    
    # Geoengineering
    'pygeoengineering': 'pygeoengineering',
    
    # Ocean engineering
    'pyoceanengineering': 'pyoceanengineering',
    
    # Coastal engineering
    'pycoastalengineering': 'pycoastalengineering',
    
    # Hydraulic engineering
    'pyhydraulicengineering': 'pyhydraulicengineering',
    
    # Structural engineering
    'pystructuralengineering': 'pystructuralengineering',
    
    # Earthquake engineering
    'pyearthquakeengineering': 'pyearthquakeengineering',
    
    # Wind engineering
    'pywindengineering': 'pywindengineering',
    
    # Fire engineering
    'pyfireengineering': 'pyfireengineering',
    
    # Blast engineering
    'pyblastengineering': 'pyblastengineering',
    
    # Protective engineering
    'pyprotectiveengineering': 'pyprotectiveengineering',
    
    # Forensic engineering
    'pyforensicengineering': 'pyforensicengineering',
    
    # Reliability engineering
    'pyreliabilityengineering': 'pyreliabilityengineering',
    
    # Safety engineering
    'pysafetyengineering': 'pysafetyengineering',
    
    # Risk engineering
    'pyriskengineering': 'pyriskengineering',
    
    # Quality engineering
    'pyqualityengineering': 'pyqualityengineering',
    
    # Manufacturing engineering
    'pymanufacturingengineering': 'pymanufacturingengineering',
    
    # Production engineering
    'pyproductionengineering': 'pyproductionengineering',
    
    # Process engineering
    'pyprocessengineering': 'pyprocessengineering',
    
    # Control engineering
    'pycontrolengineering': 'pycontrolengineering',
    
    # Instrumentation engineering
    'pyinstrumentationengineering': 'pyinstrumentationengineering',
    
    # Automation engineering
    'pyautomationengineering': 'pyautomationengineering',
    
    # Robotics engineering (additional)
    'pyroboticsengineering': 'pyroboticsengineering',
    
    # Mechatronics engineering
    'pymechatronicsengineering': 'pymechatronicsengineering',
    
    # Embedded systems engineering
    'pyembeddedsystemsengineering': 'pyembeddedsystemsengineering',
    
    # FPGA engineering
    'pyfpgaengineering': 'pyfpgaengineering',
    
    # ASIC engineering
    'pyasicengineering': 'pyasicengineering',
    
    # VLSI engineering
    'pyvlsiengineering': 'pyvlsiengineering',
    
    # Microelectronics engineering
    'pymicroelectronicsengineering': 'pymicroelectronicsengineering',
    
    # Photonics engineering
    'pyphotonicsengineering': 'pyphotonicsengineering',
    
    # Plasma engineering
    'pyplasmaengineering': 'pyplasmaengineering',
    
    # Cryogenics engineering
    'pycryogenicsengineering': 'pycryogenicsengineering',
    
    # Vacuum engineering
    'pyvacuumengineering': 'pyvacuumengineering',
    
    # Ultrasonics engineering
    'pyultrasonicsengineering': 'pyultrasonicsengineering',
    
    # Tribology engineering
    'pytribologyengineering': 'pytribologyengineering',
    
    # Corrosion engineering
    'pycorrosionengineering': 'pycorrosionengineering',
    
    # Welding engineering
    'pyweldingengineering': 'pyweldingengineering',
    
    # Casting engineering
    'pycastingengineering': 'pycastingengineering',
    
    # Forging engineering
    'pyforgingengineering': 'pyforgingengineering',
    
    # Extrusion engineering
    'pyextrusionengineering': 'pyextrusionengineering',
    
    # Molding engineering
    'pymoldingengineering': 'pymoldingengineering',
    
    # Machining engineering
    'pymachiningengineering': 'pymachiningengineering',
    
    # Grinding engineering
    'pygrindingengineering': 'pygrindingengineering',
    
    # Cutting engineering
    'pycuttingengineering': 'pycuttingengineering',
    
    # Forming engineering
    'pyformingengineering': 'pyformingengineering',
    
    # Joining engineering
    'pyjoiningengineering': 'pyjoiningengineering',
    
    # Assembly engineering
    'pyassemblyengineering': 'pyassemblyengineering',
    
    # Packaging engineering
    'pypackagingengineering': 'pypackagingengineering',
    
    # Logistics engineering
    'pylogisticsengineering': 'pylogisticsengineering',
    
    # Supply chain engineering
    'pysupplychainengineering': 'pysupplychainengineering',
    
    # Transportation engineering
    'pytransportationengineering': 'pytransportationengineering',
    
    # Highway engineering
    'pyhighwayengineering': 'pyhighwayengineering',
    
    # Railway engineering
    'pyrailwayengineering': 'pyrailwayengineering',
    
    # Airport engineering
    'pyairportengineering': 'pyairportengineering',
    
    # Port engineering
    'pyportengineering': 'pyportengineering',
    
    # Tunnel engineering
    'pytunnelengineering': 'pytunnelengineering',
    
    # Bridge engineering
    'pybridgeengineering': 'pybridgeengineering',
    
    # Dam engineering
    'pydamengineering': 'pydamengineering',
    
    # Levee engineering
    'pyleveeengineering': 'pyleveeengineering',
    
    # Canal engineering
    'pycanalengineering': 'pycanalengineering',
    
    # Pipeline engineering
    'pypipelineengineering': 'pypipelineengineering',
    
    # Power plant engineering
    'pypowerplantengineering': 'pypowerplantengineering',
    
    # Renewable energy plant engineering
    'pyrenewableenergyplantengineering': 'pyrenewableenergyplantengineering',
    
    # Waste treatment plant engineering
    'pywastetreatmentplantengineering': 'pywastetreatmentplantengineering',
    
    # Water treatment plant engineering
    'pywatertreatmentplantengineering': 'pywatertreatmentplantengineering',
    
    # Desalination plant engineering
    'pydesalinationplantengineering': 'pydesalinationplantengineering',
    
    # Chemical plant engineering
    'pychemicalplantengineering': 'pychemicalplantengineering',
    
# --- End Automatic Package Installation & Script Running ---


# --- Database Operations ---
DB_LOCK = threading.Lock()

def save_user_file(user_id, file_name, file_type='py'):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                      (user_id, file_name, file_type))
            conn.commit()
            if user_id not in user_files: user_files[user_id] = []
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
            user_files[user_id].append((file_name, file_type))
            logger.info(f"Saved file '{file_name}' ({file_type}) for user {user_id}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving file for user {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def remove_user_file_db(user_id, file_name):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            if user_id in user_files:
                user_files[user_id] = [f for f in user_files[user_id] if f[0] != file_name]
                if not user_files[user_id]: del user_files[user_id]
            logger.info(f"Removed file '{file_name}' for user {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing file for {user_id}, {file_name}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing file for {user_id}, {file_name}: {e}", exc_info=True)
        finally: conn.close()

def add_active_user(user_id):
    active_users.add(user_id)
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
            conn.commit()
            logger.info(f"Added/Confirmed active user {user_id} in DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding active user {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding active user {user_id}: {e}", exc_info=True)
        finally: conn.close()

def save_subscription(user_id, expiry):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            expiry_str = expiry.isoformat()
            c.execute('INSERT OR REPLACE INTO subscriptions (user_id, expiry) VALUES (?, ?)', (user_id, expiry_str))
            conn.commit()
            user_subscriptions[user_id] = {'expiry': expiry}
            logger.info(f"Saved subscription for {user_id}, expiry {expiry_str}")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error saving subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error saving subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_subscription_db(user_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('DELETE FROM subscriptions WHERE user_id = ?', (user_id,))
            conn.commit()
            if user_id in user_subscriptions: del user_subscriptions[user_id]
            logger.info(f"Removed subscription for {user_id} from DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing subscription for {user_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error removing subscription for {user_id}: {e}", exc_info=True)
        finally: conn.close()

def add_admin_db(admin_id):
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        try:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (admin_id,))
            conn.commit()
            admin_ids.add(admin_id)
            logger.info(f"Added admin {admin_id} to DB")
        except sqlite3.Error as e: logger.error(f"❌ SQLite error adding admin {admin_id}: {e}")
        except Exception as e: logger.error(f"❌ Unexpected error adding admin {admin_id}: {e}", exc_info=True)
        finally: conn.close()

def remove_admin_db(admin_id):
    if admin_id == OWNER_ID:
        logger.warning("Attempted to remove OWNER_ID from admins.")
        return False
    with DB_LOCK:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        removed = False
        try:
            c.execute('SELECT 1 FROM admins WHERE user_id = ?', (admin_id,))
            if c.fetchone():
                c.execute('DELETE FROM admins WHERE user_id = ?', (admin_id,))
                conn.commit()
                removed = c.rowcount > 0
                if removed: admin_ids.discard(admin_id); logger.info(f"Removed admin {admin_id} from DB")
                else: logger.warning(f"Admin {admin_id} found but delete affected 0 rows.")
            else:
                logger.warning(f"Admin {admin_id} not found in DB.")
                admin_ids.discard(admin_id)
            return removed
        except sqlite3.Error as e: logger.error(f"❌ SQLite error removing admin {admin_id}: {e}"); return False
        except Exception as e: logger.error(f"❌ Unexpected error removing admin {admin_id}: {e}", exc_info=True); return False
        finally: conn.close()
# --- End Database Operations ---

# --- Menu creation (Inline and ReplyKeyboards) ---
def create_main_menu_inline(user_id):
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton('📢 Updates Channel', url=UPDATE_CHANNEL),
        types.InlineKeyboardButton('📤 Upload File', callback_data='upload'),
        types.InlineKeyboardButton('📂 Check Files', callback_data='check_files'),
        types.InlineKeyboardButton('⚡ Bot Speed', callback_data='speed'),
        types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}')
    ]

    if user_id in admin_ids:
        admin_buttons = [
            types.InlineKeyboardButton('💳 Subscriptions', callback_data='subscription'), #0
            types.InlineKeyboardButton('📊 Statistics', callback_data='stats'), #1
            types.InlineKeyboardButton('🔒 Lock Bot' if not bot_locked else '🔓 Unlock Bot', #2
                                     callback_data='lock_bot' if not bot_locked else 'unlock_bot'),
            types.InlineKeyboardButton('📢 Broadcast', callback_data='broadcast'), #3
            types.InlineKeyboardButton('👑 Admin Panel', callback_data='admin_panel'), #4
            types.InlineKeyboardButton('🟢 Run All User Scripts', callback_data='run_all_scripts') #5
        ]
        markup.add(buttons[0]) # Updates
        markup.add(buttons[1], buttons[2]) # Upload, Check Files
        markup.add(buttons[3], admin_buttons[0]) # Speed, Subscriptions
        markup.add(admin_buttons[1], admin_buttons[3]) # Stats, Broadcast
        markup.add(admin_buttons[2], admin_buttons[5]) # Lock Bot, Run All Scripts
        markup.add(admin_buttons[4]) # Admin Panel
        markup.add(buttons[4]) # Contact
    else:
        markup.add(buttons[0])
        markup.add(buttons[1], buttons[2])
        markup.add(buttons[3])
        markup.add(types.InlineKeyboardButton('📊 Statistics', callback_data='stats')) # Allow non-admins to see stats too
        markup.add(buttons[4])
    return markup

def create_reply_keyboard_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    layout_to_use = ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC if user_id in admin_ids else COMMAND_BUTTONS_LAYOUT_USER_SPEC
    for row_buttons_text in layout_to_use:
        markup.add(*[types.KeyboardButton(text) for text in row_buttons_text])
    return markup

def create_control_buttons(script_owner_id, file_name, is_running=True): # Parameter renamed
    markup = types.InlineKeyboardMarkup(row_width=2)
    # Callbacks use script_owner_id
    if is_running:
        markup.row(
            types.InlineKeyboardButton("🔴 Stop", callback_data=f'stop_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🔄 Restart", callback_data=f'restart_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("📜 Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    else:
        markup.row(
            types.InlineKeyboardButton("🟢 Start", callback_data=f'start_{script_owner_id}_{file_name}'),
            types.InlineKeyboardButton("🗑️ Delete", callback_data=f'delete_{script_owner_id}_{file_name}')
        )
        markup.row(
            types.InlineKeyboardButton("📜 View Logs", callback_data=f'logs_{script_owner_id}_{file_name}')
        )
    markup.add(types.InlineKeyboardButton("🔙 Back to Files", callback_data='check_files'))
    return markup

def create_admin_panel():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Admin', callback_data='add_admin'),
        types.InlineKeyboardButton('➖ Remove Admin', callback_data='remove_admin')
    )
    markup.row(types.InlineKeyboardButton('📋 List Admins', callback_data='list_admins'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup

def create_subscription_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton('➕ Add Subscription', callback_data='add_subscription'),
        types.InlineKeyboardButton('➖ Remove Subscription', callback_data='remove_subscription')
    )
    markup.row(types.InlineKeyboardButton('🔍 Check Subscription', callback_data='check_subscription'))
    markup.row(types.InlineKeyboardButton('🔙 Back to Main', callback_data='back_to_main'))
    return markup
# --- End Menu Creation ---

# --- File Handling ---
def handle_zip_file(downloaded_file_content, file_name_zip, message):
    user_id = message.from_user.id
    user_folder = get_user_folder(user_id)
    temp_dir = None
    
    try:
        # Check if zip file is empty
        if len(downloaded_file_content) == 0:
            bot.reply_to(message, "❌ Error: ZIP file is empty.")
            return
            
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix=f"user_{user_id}_zip_")
        logger.info(f"Temp dir for zip: {temp_dir}")
        
        # Save zip file to temp directory
        zip_path = os.path.join(temp_dir, file_name_zip)
        with open(zip_path, 'wb') as new_file:
            new_file.write(downloaded_file_content)
        
        # Check if file is a valid zip file
        try:
            # First, test if it's a valid zip file
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Test the zip file
                test_result = zip_ref.testzip()
                if test_result is not None:
                    bot.reply_to(message, f"❌ Error: ZIP file is corrupted or invalid. First bad file: {test_result}")
                    return
                
                # Security check for path traversal
                for member in zip_ref.infolist():
                    # Skip directory entries
                    if member.filename.endswith('/'):
                        continue
                        
                    member_path = os.path.abspath(os.path.join(temp_dir, member.filename))
                    if not member_path.startswith(os.path.abspath(temp_dir)):
                        bot.reply_to(message, "❌ Error: ZIP file contains unsafe paths.")
                        return
                
                # Extract all files
                zip_ref.extractall(temp_dir)
                logger.info(f"Extracted zip to {temp_dir}")
                
        except zipfile.BadZipFile as e:
            bot.reply_to(message, f"❌ Error: Invalid ZIP file format. {str(e)}")
            return
        except Exception as e:
            bot.reply_to(message, f"❌ Error reading ZIP file: {str(e)}")
            return
        
        # Get all extracted files recursively
        def find_all_files(directory):
            all_files = []
            for root, dirs, files in os.walk(directory):
                for file in files:
                    full_path = os.path.join(root, file)
                    rel_path = os.path.relpath(full_path, directory)
                    all_files.append((full_path, rel_path))
            return all_files
        
        all_extracted_files = find_all_files(temp_dir)
        
        # Find Python and JS files
        py_files = []
        js_files = []
        
        for full_path, rel_path in all_extracted_files:
            if rel_path.endswith('.py'):
                py_files.append((full_path, rel_path))
            elif rel_path.endswith('.js'):
                js_files.append((full_path, rel_path))
        
        # Log found files for debugging
        logger.info(f"Total extracted items: {len(all_extracted_files)}")
        logger.info(f"Python files found: {len(py_files)}")
        logger.info(f"JS files found: {len(js_files)}")
        
        # Check for requirements.txt and package.json
        req_file = None
        pkg_json = None
        
        for full_path, rel_path in all_extracted_files:
            if rel_path == 'requirements.txt':
                req_file = (full_path, rel_path)
            elif rel_path == 'package.json':
                pkg_json = (full_path, rel_path)
        
        # Install Python dependencies if requirements.txt exists
        if req_file:
            req_path, req_name = req_file
            logger.info(f"requirements.txt found, installing: {req_path}")
            bot.reply_to(message, f"🔄 Installing Python dependencies from `{req_name}`...")
            try:
                command = [sys.executable, '-m', 'pip', 'install', '-r', req_path]
                result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8', errors='ignore')
                logger.info(f"pip install from requirements.txt OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Python dependencies from `{req_name}` installed.")
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Python dependencies from `{req_name}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown')
                return
            except Exception as e:
                error_msg = f"❌ Unexpected error installing Python dependencies: {e}"
                logger.error(error_msg, exc_info=True)
                bot.reply_to(message, error_msg)
                return
        
        # Install Node dependencies if package.json exists
        if pkg_json:
            pkg_path, pkg_name = pkg_json
            logger.info(f"package.json found, npm install in: {temp_dir}")
            bot.reply_to(message, f"🔄 Installing Node dependencies from `{pkg_name}`...")
            try:
                command = ['npm', 'install']
                result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=temp_dir, encoding='utf-8', errors='ignore')
                logger.info(f"npm install OK. Output:\n{result.stdout}")
                bot.reply_to(message, f"✅ Node dependencies from `{pkg_name}` installed.")
            except FileNotFoundError:
                bot.reply_to(message, "❌ 'npm' not found. Cannot install Node dependencies.")
                return
            except subprocess.CalledProcessError as e:
                error_msg = f"❌ Failed to install Node dependencies from `{pkg_name}`.\nLog:\n```\n{e.stderr or e.stdout}\n```"
                logger.error(error_msg)
                if len(error_msg) > 4000: error_msg = error_msg[:4000] + "\n... (Log truncated)"
                bot.reply_to(message, error_msg, parse_mode='Markdown')
                return
            except Exception as e:
                error_msg = f"❌ Unexpected error installing Node dependencies: {e}"
                logger.error(error_msg, exc_info=True)
                bot.reply_to(message, error_msg)
                return
        
        # Find main script
        main_script_path = None
        main_script_name = None
        file_type = None
        
        # Preferred script names
        preferred_py = ['main.py', 'bot.py', 'app.py', 'start.py', 'index.py']
        preferred_js = ['index.js', 'main.js', 'bot.js', 'app.js', 'start.js']
        
        # First look for preferred Python scripts
        for full_path, rel_path in py_files:
            for preferred in preferred_py:
                if rel_path.endswith(preferred) or rel_path == preferred:
                    main_script_path = full_path
                    main_script_name = os.path.basename(rel_path)
                    file_type = 'py'
                    break
            if main_script_path:
                break
        
        # If no preferred Python script, look for any Python script
        if not main_script_path and py_files:
            # Sort by path depth (shallow first) and then alphabetically
            py_files_sorted = sorted(py_files, key=lambda x: (x[1].count('/'), x[1]))
            main_script_path, rel_path = py_files_sorted[0]
            main_script_name = os.path.basename(rel_path)
            file_type = 'py'
        
        # If no Python script, look for preferred JS scripts
        if not main_script_path:
            for full_path, rel_path in js_files:
                for preferred in preferred_js:
                    if rel_path.endswith(preferred) or rel_path == preferred:
                        main_script_path = full_path
                        main_script_name = os.path.basename(rel_path)
                        file_type = 'js'
                        break
                if main_script_path:
                    break
        
        # If no preferred JS script, look for any JS script
        if not main_script_path and js_files:
            # Sort by path depth (shallow first) and then alphabetically
            js_files_sorted = sorted(js_files, key=lambda x: (x[1].count('/'), x[1]))
            main_script_path, rel_path = js_files_sorted[0]
            main_script_name = os.path.basename(rel_path)
            file_type = 'js'
        
        # If no script found at all
        if not main_script_path:
            # List all files found in zip for debugging
            file_list = []
            for full_path, rel_path in all_extracted_files[:10]:  # First 10 files only
                file_list.append(f"• {rel_path}")
            
            file_list_str = "\n".join(file_list)
            if len(all_extracted_files) > 10:
                file_list_str += f"\n... and {len(all_extracted_files) - 10} more files"
            
            error_msg = (
                "❌ No `.py` or `.js` script files found in the ZIP archive.\n\n"
                "Please ensure your ZIP file contains at least one Python (.py) or JavaScript (.js) file.\n\n"
                f"Files found in ZIP:\n{file_list_str if file_list_str else 'No files found'}"
            )
            bot.reply_to(message, error_msg)
            return
        
        # Move all extracted files to user folder
        logger.info(f"Moving extracted files from {temp_dir} to {user_folder}")
        moved_count = 0
        
        for full_path, rel_path in all_extracted_files:
            dest_path = os.path.join(user_folder, rel_path)
            
            # Create destination directory if it doesn't exist
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            # Remove existing file/directory if it exists
            if os.path.exists(dest_path):
                if os.path.isdir(dest_path):
                    shutil.rmtree(dest_path)
                else:
                    os.remove(dest_path)
            
            # Move file
            shutil.move(full_path, dest_path)
            moved_count += 1
        
        logger.info(f"Moved {moved_count} items to {user_folder}")
        
        # Calculate relative path for main script in user folder
        main_script_rel_path = os.path.relpath(main_script_path, temp_dir)
        main_script_dest_path = os.path.join(user_folder, main_script_rel_path)
        
        # Save file info to database
        save_user_file(user_id, main_script_name, file_type)
        logger.info(f"Saved main script '{main_script_name}' ({file_type}) for {user_id} from zip.")
        
        # Start the main script
        bot.reply_to(message, f"✅ Files extracted successfully! Starting main script: `{main_script_name}`...", parse_mode='Markdown')
        
        # Use user_id as script_owner_id for script key context
        if file_type == 'py':
            threading.Thread(target=run_script, args=(main_script_dest_path, user_id, user_folder, main_script_name, message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(main_script_dest_path, user_id, user_folder, main_script_name, message)).start()
        
    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file from {user_id}: {e}")
        bot.reply_to(message, f"❌ Error: Invalid or corrupted ZIP file. Please check your ZIP file and try again.")
    except Exception as e:
        logger.error(f"❌ Error processing zip for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing zip file: {str(e)}")
    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Cleaned temp dir: {temp_dir}")
            except Exception as e:
                logger.error(f"Failed to clean temp dir {temp_dir}: {e}", exc_info=True)

def handle_js_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'js')
        threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing JS file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing JS file: {str(e)}")

def handle_py_file(file_path, script_owner_id, user_folder, file_name, message):
    try:
        save_user_file(script_owner_id, file_name, 'py')
        threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, message)).start()
    except Exception as e:
        logger.error(f"❌ Error processing Python file {file_name} for {script_owner_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Error processing Python file: {str(e)}")
# --- End File Handling ---


# --- Logic Functions (called by commands and text handlers) ---
def _logic_send_welcome(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_name = message.from_user.first_name
    user_username = message.from_user.username

    logger.info(f"Welcome request from user_id: {user_id}, username: @{user_username}")

    if bot_locked and user_id not in admin_ids:
        bot.send_message(chat_id, "⚠️ Bot locked by admin. Try later.")
        return

    user_bio = "Could not fetch bio"; photo_file_id = None
    try: user_bio = bot.get_chat(user_id).bio or "No bio"
    except Exception: pass
    try:
        user_profile_photos = bot.get_user_profile_photos(user_id, limit=1)
        if user_profile_photos.photos: photo_file_id = user_profile_photos.photos[0][-1].file_id
    except Exception: pass

    if user_id not in active_users:
        add_active_user(user_id)
        try:
            owner_notification = (f"🎉 New user!\n👤 Name: {user_name}\n✳️ User: @{user_username or 'N/A'}\n"
                                  f"🆔 ID: `{user_id}`\n📝 Bio: {user_bio}")
            bot.send_message(OWNER_ID, owner_notification, parse_mode='Markdown')
            if photo_file_id: bot.send_photo(OWNER_ID, photo_file_id, caption=f"Pic of new user {user_id}")
        except Exception as e: logger.error(f"⚠️ Failed to notify owner about new user {user_id}: {e}")

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    if user_id == OWNER_ID: user_status = "👑 Owner"
    elif user_id in admin_ids: user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"; days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: user_status = "🆓 Free User (Expired Sub)"; remove_subscription_db(user_id) # Clean up expired
    else: user_status = "🆓 Free User"

    welcome_msg_text = (f"〽️ Welcome, {user_name}!\n\n🆔 Your User ID: `{user_id}`\n"
                        f"✳️ Username: `@{user_username or 'Not set'}`\n"
                        f"🔰 Your Status: {user_status}{expiry_info}\n"
                        f"📁 Files Uploaded: {current_files} / {limit_str}\n\n"
                        f"🤖 Host & run Python (`.py`) or JS (`.js`) scripts.\n"
                        f"   Upload single scripts or `.zip` archives.\n\n"
                        f"👇 Use buttons or type commands.")
    main_reply_markup = create_reply_keyboard_main_menu(user_id)
    try:
        if photo_file_id: bot.send_photo(chat_id, photo_file_id)
        bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}", exc_info=True)
        try: bot.send_message(chat_id, welcome_msg_text, reply_markup=main_reply_markup, parse_mode='Markdown') # Fallback without photo
        except Exception as fallback_e: logger.error(f"Fallback send_message failed for {user_id}: {fallback_e}")

def _logic_updates_channel(message):
    bot.send_message(
        message.chat.id,
        "📢 Click below to join our Updates Channel 👇",
        reply_markup=types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("📢 Join Now", url=f'https://t.me/{UPDATE_CHANNEL.replace("@", "")}')
        )
    )

def _logic_upload_file(message):
    user_id = message.from_user.id
    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked by admin, cannot accept files.")
        return

    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files first.")
        return
    bot.reply_to(message, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def _logic_check_files(message):
    user_id = message.from_user.id
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.reply_to(message, "📂 Your files:\n\n(No files uploaded yet)")
        return
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name) # Use user_id for checking status
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    bot.reply_to(message, "📂 Your files:\nClick to manage.", reply_markup=markup, parse_mode='Markdown')

def _logic_bot_speed(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    start_time_ping = time.time()
    wait_msg = bot.reply_to(message, "🏃 Testing speed...")
    try:
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_time_ping) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        if user_id == OWNER_ID: user_level = "👑 Owner"
        elif user_id in admin_ids: user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): user_level = "⭐ Premium"
        else: user_level = "🆓 Free User"
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        bot.edit_message_text(speed_msg, chat_id, wait_msg.message_id)
    except Exception as e:
        logger.error(f"Error during speed test (cmd): {e}", exc_info=True)
        bot.edit_message_text("❌ Error during speed test.", chat_id, wait_msg.message_id)

def _logic_contact_owner(message):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton('📞 Contact Owner', url=f'https://t.me/{YOUR_USERNAME.replace("@", "")}'))
    bot.reply_to(message, "Click to contact Owner:", reply_markup=markup)

# --- Admin Logic Functions ---
def _logic_subscriptions_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "💳 Subscription Management\nUse inline buttons from /start or admin command menu.", reply_markup=create_subscription_menu())

def _logic_statistics(message):
    # No admin check here, allow all users but show admin-specific info if admin
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files_records = sum(len(files) for files in user_files.values())

    running_bots_count = 0
    user_running_bots = 0

    for script_key_iter, script_info_iter in list(bot_scripts.items()):
        s_owner_id, _ = script_key_iter.split('_', 1) # Extract owner_id from key
        if is_bot_running(int(s_owner_id), script_info_iter['file_name']):
            running_bots_count += 1
            if int(s_owner_id) == user_id:
                user_running_bots +=1

    stats_msg_base = (f"📊 Bot Statistics:\n\n"
                      f"👥 Total Users: {total_users}\n"
                      f"📂 Total File Records: {total_files_records}\n"
                      f"🟢 Total Active Bots: {running_bots_count}\n")

    if user_id in admin_ids:
        stats_msg_admin = (f"🔒 Bot Status: {'🔴 Locked' if bot_locked else '🟢 Unlocked'}\n"
                           f"🤖 Your Running Bots: {user_running_bots}")
        stats_msg = stats_msg_base + stats_msg_admin
    else:
        stats_msg = stats_msg_base + f"🤖 Your Running Bots: {user_running_bots}"

    bot.reply_to(message, stats_msg)


def _logic_broadcast_init(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    msg = bot.reply_to(message, "📢 Send message to broadcast to all active users.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def _logic_toggle_lock_bot(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    global bot_locked
    bot_locked = not bot_locked
    status = "locked" if bot_locked else "unlocked"
    logger.warning(f"Bot {status} by Admin {message.from_user.id} via command/button.")
    bot.reply_to(message, f"🔒 Bot has been {status}.")

def _logic_admin_panel(message):
    if message.from_user.id not in admin_ids:
        bot.reply_to(message, "⚠️ Admin permissions required.")
        return
    bot.reply_to(message, "👑 Admin Panel\nManage admins. Use inline buttons from /start or admin menu.",
                 reply_markup=create_admin_panel())

def _logic_run_all_scripts(message_or_call):
    if isinstance(message_or_call, telebot.types.Message):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.chat.id
        reply_func = lambda text, **kwargs: bot.reply_to(message_or_call, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call
    elif isinstance(message_or_call, telebot.types.CallbackQuery):
        admin_user_id = message_or_call.from_user.id
        admin_chat_id = message_or_call.message.chat.id
        bot.answer_callback_query(message_or_call.id)
        reply_func = lambda text, **kwargs: bot.send_message(admin_chat_id, text, **kwargs)
        admin_message_obj_for_script_runner = message_or_call.message
    else:
        logger.error("Invalid argument for _logic_run_all_scripts")
        return

    if admin_user_id not in admin_ids:
        reply_func("⚠️ Admin permissions required.")
        return

    reply_func("⏳ Starting process to run all user scripts. This may take a while...")
    logger.info(f"Admin {admin_user_id} initiated 'run all scripts' from chat {admin_chat_id}.")

    started_count = 0; attempted_users = 0; skipped_files = 0; error_files_details = []

    # Use a copy of user_files keys and values to avoid modification issues during iteration
    all_user_files_snapshot = dict(user_files)

    for target_user_id, files_for_user in all_user_files_snapshot.items():
        if not files_for_user: continue
        attempted_users += 1
        logger.info(f"Processing scripts for user {target_user_id}...")
        user_folder = get_user_folder(target_user_id)

        for file_name, file_type in files_for_user:
            # script_owner_id for key context is target_user_id
            if not is_bot_running(target_user_id, file_name):
                file_path = os.path.join(user_folder, file_name)
                if os.path.exists(file_path):
                    logger.info(f"Admin {admin_user_id} attempting to start '{file_name}' ({file_type}) for user {target_user_id}.")
                    try:
                        if file_type == 'py':
                            threading.Thread(target=run_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        elif file_type == 'js':
                            threading.Thread(target=run_js_script, args=(file_path, target_user_id, user_folder, file_name, admin_message_obj_for_script_runner)).start()
                            started_count += 1
                        else:
                            logger.warning(f"Unknown file type '{file_type}' for {file_name} (user {target_user_id}). Skipping.")
                            error_files_details.append(f"`{file_name}` (User {target_user_id}) - Unknown type")
                            skipped_files += 1
                        time.sleep(0.7) # Increased delay slightly
                    except Exception as e:
                        logger.error(f"Error queueing start for '{file_name}' (user {target_user_id}): {e}")
                        error_files_details.append(f"`{file_name}` (User {target_user_id}) - Start error")
                        skipped_files += 1
                else:
                    logger.warning(f"File '{file_name}' for user {target_user_id} not found at '{file_path}'. Skipping.")
                    error_files_details.append(f"`{file_name}` (User {target_user_id}) - File not found")
                    skipped_files += 1

    summary_msg = (f"✅ All Users' Scripts - Processing Complete:\n\n"
                   f"▶️ Attempted to start: {started_count} scripts.\n"
                   f"👥 Users processed: {attempted_users}.\n")
    if skipped_files > 0:
        summary_msg += f"⚠️ Skipped/Error files: {skipped_files}\n"
        if error_files_details:
             summary_msg += "Details (first 5):\n" + "\n".join([f"  - {err}" for err in error_files_details[:5]])
             if len(error_files_details) > 5: summary_msg += "\n  ... and more (check logs)."

    reply_func(summary_msg, parse_mode='Markdown')
    logger.info(f"Run all scripts finished. Admin: {admin_user_id}. Started: {started_count}. Skipped/Errors: {skipped_files}")


# --- Command Handlers & Text Handlers for ReplyKeyboard ---
@bot.message_handler(commands=['start', 'help'])
def command_send_welcome(message): _logic_send_welcome(message)

@bot.message_handler(commands=['status']) # Kept for direct command
def command_show_status(message): _logic_statistics(message) # Changed to call _logic_statistics


BUTTON_TEXT_TO_LOGIC = {
    "📢 Updates Channel": _logic_updates_channel,
    "📤 Upload File": _logic_upload_file,
    "📂 Check Files": _logic_check_files,
    "⚡ Bot Speed": _logic_bot_speed,
    "📞 Contact Owner": _logic_contact_owner,
    "📊 Statistics": _logic_statistics,
    "💳 Subscriptions": _logic_subscriptions_panel,
    "📢 Broadcast": _logic_broadcast_init,
    "🔒 Lock Bot": _logic_toggle_lock_bot,
    "🟢 Running All Code": _logic_run_all_scripts, # Added
    "👑 Admin Panel": _logic_admin_panel,
}

@bot.message_handler(func=lambda message: message.text in BUTTON_TEXT_TO_LOGIC)
def handle_button_text(message):
    logic_func = BUTTON_TEXT_TO_LOGIC.get(message.text)
    if logic_func: logic_func(message)
    else: logger.warning(f"Button text '{message.text}' matched but no logic func.")

@bot.message_handler(commands=['updateschannel'])
def command_updates_channel(message): _logic_updates_channel(message)
@bot.message_handler(commands=['uploadfile'])
def command_upload_file(message): _logic_upload_file(message)
@bot.message_handler(commands=['checkfiles'])
def command_check_files(message): _logic_check_files(message)
@bot.message_handler(commands=['botspeed'])
def command_bot_speed(message): _logic_bot_speed(message)
@bot.message_handler(commands=['contactowner'])
def command_contact_owner(message): _logic_contact_owner(message)
@bot.message_handler(commands=['subscriptions'])
def command_subscriptions(message): _logic_subscriptions_panel(message)
@bot.message_handler(commands=['statistics']) # Alias for /status
def command_statistics(message): _logic_statistics(message)
@bot.message_handler(commands=['broadcast'])
def command_broadcast(message): _logic_broadcast_init(message)
@bot.message_handler(commands=['lockbot'])
def command_lock_bot(message): _logic_toggle_lock_bot(message)
@bot.message_handler(commands=['adminpanel'])
def command_admin_panel(message): _logic_admin_panel(message)
@bot.message_handler(commands=['runningallcode']) # Added
def command_run_all_code(message): _logic_run_all_scripts(message)


@bot.message_handler(commands=['ping'])
def ping(message):
    start_ping_time = time.time()
    msg = bot.reply_to(message, "Pong!")
    latency = round((time.time() - start_ping_time) * 1000, 2)
    bot.edit_message_text(f"Pong! Latency: {latency} ms", message.chat.id, msg.message_id)


# --- Document (File) Handler ---
@bot.message_handler(content_types=['document'])
def handle_file_upload_doc(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    doc = message.document
    logger.info(f"Document from {user_id}: {doc.file_name} ({doc.mime_type}), Size: {doc.file_size}")

    if bot_locked and user_id not in admin_ids:
        bot.reply_to(message, "⚠️ Bot locked, cannot accept files.")
        return

    # File limit check
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.reply_to(message, f"⚠️ File limit ({current_files}/{limit_str}) reached. Delete files via /checkfiles.")
        return

    file_name = doc.file_name
    if not file_name: 
        bot.reply_to(message, "⚠️ No file name. Ensure file has a name.")
        return
    
    file_ext = os.path.splitext(file_name)[1].lower()
    if file_ext not in ['.py', '.js', '.zip']:
        bot.reply_to(message, "⚠️ Unsupported file type! Only `.py`, `.js`, or `.zip` files are allowed.")
        return
    
    max_file_size = 20 * 1024 * 1024 # 20 MB
    if doc.file_size > max_file_size:
        bot.reply_to(message, f"⚠️ File too large (Max: {max_file_size // 1024 // 1024} MB).")
        return

    try:
        # Forward file to owner for logging
        try:
            bot.forward_message(OWNER_ID, chat_id, message.message_id)
            bot.send_message(OWNER_ID, f"⬆️ File '{file_name}' from {message.from_user.first_name} (`{user_id}`)", parse_mode='Markdown')
        except Exception as e: 
            logger.error(f"Failed to forward uploaded file to OWNER_ID {OWNER_ID}: {e}")

        download_wait_msg = bot.reply_to(message, f"⏳ Downloading `{file_name}`...")
        
        try:
            file_info = bot.get_file(doc.file_id)
            downloaded_file_content = bot.download_file(file_info.file_path)
            bot.edit_message_text(f"✅ Downloaded `{file_name}`. Processing...", chat_id, download_wait_msg.message_id)
            logger.info(f"Downloaded {file_name} for user {user_id}")
        except Exception as e:
            bot.edit_message_text(f"❌ Failed to download file: {str(e)}", chat_id, download_wait_msg.message_id)
            return

        user_folder = get_user_folder(user_id)

        if file_ext == '.zip':
            handle_zip_file(downloaded_file_content, file_name, message)
        else:
            file_path = os.path.join(user_folder, file_name)
            with open(file_path, 'wb') as f: 
                f.write(downloaded_file_content)
            logger.info(f"Saved single file to {file_path}")
            
            # Pass user_id as script_owner_id
            if file_ext == '.js': 
                handle_js_file(file_path, user_id, user_folder, file_name, message)
            elif file_ext == '.py': 
                handle_py_file(file_path, user_id, user_folder, file_name, message)
                
    except telebot.apihelper.ApiTelegramException as e:
         logger.error(f"Telegram API Error handling file for {user_id}: {e}", exc_info=True)
         if "file is too big" in str(e).lower():
              bot.reply_to(message, f"❌ Telegram API Error: File too large to download (~20MB limit).")
         else: 
             bot.reply_to(message, f"❌ Telegram API Error: {str(e)}. Try later.")
    except Exception as e:
        logger.error(f"❌ General error handling file for {user_id}: {e}", exc_info=True)
        bot.reply_to(message, f"❌ Unexpected error: {str(e)}")
# --- End Document Handler ---


# --- Callback Query Handlers (for Inline Buttons) ---
@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    user_id = call.from_user.id
    data = call.data
    logger.info(f"Callback: User={user_id}, Data='{data}'")

    if bot_locked and user_id not in admin_ids and data not in ['back_to_main', 'speed', 'stats']: # Allow stats
        bot.answer_callback_query(call.id, "⚠️ Bot locked by admin.", show_alert=True)
        return
    
    try:
        if data == 'upload': upload_callback(call)
        elif data == 'check_files': check_files_callback(call)
        elif data.startswith('file_'): file_control_callback(call)
        elif data.startswith('start_'): start_bot_callback(call)
        elif data.startswith('stop_'): stop_bot_callback(call)
        elif data.startswith('restart_'): restart_bot_callback(call)
        elif data.startswith('delete_'): delete_bot_callback(call)
        elif data.startswith('logs_'): logs_bot_callback(call)
        elif data == 'speed': speed_callback(call)
        elif data == 'back_to_main': back_to_main_callback(call)
        elif data.startswith('confirm_broadcast_'): handle_confirm_broadcast(call)
        elif data == 'cancel_broadcast': handle_cancel_broadcast(call)
        # --- Admin Callbacks ---
        elif data == 'subscription': admin_required_callback(call, subscription_management_callback)
        elif data == 'stats': stats_callback(call)
        elif data == 'lock_bot': admin_required_callback(call, lock_bot_callback)
        elif data == 'unlock_bot': admin_required_callback(call, unlock_bot_callback)
        elif data == 'run_all_scripts': admin_required_callback(call, run_all_scripts_callback)
        elif data == 'broadcast': admin_required_callback(call, broadcast_init_callback)
        elif data == 'admin_panel': admin_required_callback(call, admin_panel_callback)
        elif data == 'add_admin': owner_required_callback(call, add_admin_init_callback)
        elif data == 'remove_admin': owner_required_callback(call, remove_admin_init_callback)
        elif data == 'list_admins': admin_required_callback(call, list_admins_callback)
        elif data == 'add_subscription': admin_required_callback(call, add_subscription_init_callback)
        elif data == 'remove_subscription': admin_required_callback(call, remove_subscription_init_callback)
        elif data == 'check_subscription': admin_required_callback(call, check_subscription_init_callback)
        else:
            bot.answer_callback_query(call.id, "Unknown action.")
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
    except Exception as e:
        logger.error(f"Error handling callback '{data}' for {user_id}: {e}", exc_info=True)
        try: 
            bot.answer_callback_query(call.id, "Error processing request.", show_alert=True)
        except Exception as e_ans: 
            logger.error(f"Failed to answer callback after error: {e_ans}")

def admin_required_callback(call, func_to_run):
    if call.from_user.id not in admin_ids:
        bot.answer_callback_query(call.id, "⚠️ Admin permissions required.", show_alert=True)
        return
    func_to_run(call)

def owner_required_callback(call, func_to_run):
    if call.from_user.id != OWNER_ID:
        bot.answer_callback_query(call.id, "⚠️ Owner permissions required.", show_alert=True)
        return
    func_to_run(call)

def upload_callback(call):
    user_id = call.from_user.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    if current_files >= file_limit:
        limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
        bot.answer_callback_query(call.id, f"⚠️ File limit ({current_files}/{limit_str}) reached.", show_alert=True)
        return
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "📤 Send your Python (`.py`), JS (`.js`), or ZIP (`.zip`) file.")

def check_files_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    user_files_list = user_files.get(user_id, [])
    if not user_files_list:
        bot.answer_callback_query(call.id, "⚠️ No files uploaded.", show_alert=True)
        try:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
            bot.edit_message_text("📂 Your files:\n\n(No files uploaded)", chat_id, call.message.message_id, reply_markup=markup)
        except Exception as e: 
            logger.error(f"Error editing msg for empty file list: {e}")
        return
    
    bot.answer_callback_query(call.id)
    markup = types.InlineKeyboardMarkup(row_width=1)
    for file_name, file_type in sorted(user_files_list):
        is_running = is_bot_running(user_id, file_name)
        status_icon = "🟢 Running" if is_running else "🔴 Stopped"
        btn_text = f"{file_name} ({file_type}) - {status_icon}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f'file_{user_id}_{file_name}'))
    
    markup.add(types.InlineKeyboardButton("🔙 Back to Main", callback_data='back_to_main'))
    
    try:
        bot.edit_message_text("📂 Your files:\nClick to manage.", chat_id, call.message.message_id, reply_markup=markup, parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): 
             logger.warning("Message not modified (files).")
         else: 
             logger.error(f"Error editing message for file list: {e}")
    except Exception as e: 
        logger.error(f"Unexpected error editing message for file list: {e}", exc_info=True)

def file_control_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id

        # Allow owner/admin to control any file, or user to control their own
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            logger.warning(f"User {requesting_user_id} tried to access file '{file_name}' of user {script_owner_id} without permission.")
            bot.answer_callback_query(call.id, "⚠️ You can only manage your own files.", show_alert=True)
            check_files_callback(call) # Show their own files
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            logger.warning(f"File '{file_name}' not found for user {script_owner_id} during control.")
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        bot.answer_callback_query(call.id)
        is_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_running else '🔴 Stopped'
        file_type = next((f[1] for f in user_files_list if f[0] == file_name), '?')
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                call.message.chat.id, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_running),
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): 
                 logger.warning(f"Message not modified (controls for {file_name})")
             else: 
                 raise
    except (ValueError, IndexError) as ve:
        logger.error(f"Error parsing file control callback: {ve}. Data: '{call.data}'")
        bot.answer_callback_query(call.id, "Error: Invalid action data.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in file_control_callback for data '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)

def start_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Start request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")

        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied to start this script.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            check_files_callback(call)
            return

        if is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already running.", show_alert=True)
            try: 
                bot.edit_message_reply_markup(chat_id_for_reply, call.message.message_id, 
                                              reply_markup=create_control_buttons(script_owner_id, file_name, True))
            except Exception as e: 
                logger.error(f"Error updating buttons (already running): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Attempting to start {file_name} for user {script_owner_id}...")

        # Pass call.message as message_obj_for_reply
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Error: Unknown file type '{file_type}' for '{file_name}'.")
             return

        time.sleep(1.5) # Give script time to actually start or fail early
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed, check logs/replies)'
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), 
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): 
                 logger.warning(f"Message not modified after starting {file_name}")
             else: 
                 raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing start callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid start command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in start_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error starting script.", show_alert=True)
        try: # Attempt to reset buttons to 'stopped' state on error
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 
                                          reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: 
            logger.error(f"Failed to update buttons after start error: {e_btn}")

def stop_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Stop request: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        file_type = file_info[1]
        script_key = f"{script_owner_id}_{file_name}"

        if not is_bot_running(script_owner_id, file_name):
            bot.answer_callback_query(call.id, f"⚠️ Script '{file_name}' already stopped.", show_alert=True)
            try:
                 bot.edit_message_text(
                     f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                     chat_id_for_reply, call.message.message_id,
                     reply_markup=create_control_buttons(script_owner_id, file_name, False), 
                     parse_mode='Markdown')
            except Exception as e: 
                logger.error(f"Error updating buttons (already stopped): {e}")
            return

        bot.answer_callback_query(call.id, f"⏳ Stopping {file_name} for user {script_owner_id}...")
        process_info = bot_scripts.get(script_key)
        if process_info:
            kill_process_tree(process_info)
            if script_key in bot_scripts: 
                del bot_scripts[script_key]
                logger.info(f"Removed {script_key} from running after stop.")
        else: 
            logger.warning(f"Script {script_key} running by psutil but not in bot_scripts dict.")

        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: 🔴 Stopped",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, False), 
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): 
                 logger.warning(f"Message not modified after stopping {file_name}")
             else: 
                 raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing stop callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid stop command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in stop_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error stopping script.", show_alert=True)

def restart_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Restart: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        if not file_info:
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        file_type = file_info[1]
        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        script_key = f"{script_owner_id}_{file_name}"

        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, f"⚠️ Error: File `{file_name}` missing! Re-upload.", show_alert=True)
            remove_user_file_db(script_owner_id, file_name)
            if script_key in bot_scripts: 
                del bot_scripts[script_key]
            check_files_callback(call)
            return

        bot.answer_callback_query(call.id, f"⏳ Restarting {file_name} for user {script_owner_id}...")
        
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Restart: Stopping existing {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: 
                kill_process_tree(process_info)
            if script_key in bot_scripts: 
                del bot_scripts[script_key]
            time.sleep(1.5)

        logger.info(f"Restart: Starting script {script_key}...")
        if file_type == 'py':
            threading.Thread(target=run_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        elif file_type == 'js':
            threading.Thread(target=run_js_script, args=(file_path, script_owner_id, user_folder, file_name, call.message)).start()
        else:
             bot.send_message(chat_id_for_reply, f"❌ Unknown type '{file_type}' for '{file_name}'.")
             return

        time.sleep(1.5)
        is_now_running = is_bot_running(script_owner_id, file_name)
        status_text = '🟢 Running' if is_now_running else '🟡 Starting (or failed)'
        
        try:
            bot.edit_message_text(
                f"⚙️ Controls for: `{file_name}` ({file_type}) of User `{script_owner_id}`\nStatus: {status_text}",
                chat_id_for_reply, call.message.message_id,
                reply_markup=create_control_buttons(script_owner_id, file_name, is_now_running), 
                parse_mode='Markdown'
            )
        except telebot.apihelper.ApiTelegramException as e:
             if "message is not modified" in str(e): 
                 logger.warning(f"Message not modified (restart {file_name})")
             else: 
                 raise
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing restart callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid restart command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in restart_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error restarting.", show_alert=True)
        try:
            _, script_owner_id_err_str, file_name_err = call.data.split('_', 2)
            script_owner_id_err = int(script_owner_id_err_str)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 
                                          reply_markup=create_control_buttons(script_owner_id_err, file_name_err, False))
        except Exception as e_btn: 
            logger.error(f"Failed to update buttons after restart error: {e_btn}")

def delete_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Delete: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        bot.answer_callback_query(call.id, f"🗑️ Deleting {file_name} for user {script_owner_id}...")
        script_key = f"{script_owner_id}_{file_name}"
        
        if is_bot_running(script_owner_id, file_name):
            logger.info(f"Delete: Stopping {script_key}...")
            process_info = bot_scripts.get(script_key)
            if process_info: 
                kill_process_tree(process_info)
            if script_key in bot_scripts: 
                del bot_scripts[script_key]
            time.sleep(0.5)

        user_folder = get_user_folder(script_owner_id)
        file_path = os.path.join(user_folder, file_name)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        deleted_disk = []
        
        if os.path.exists(file_path):
            try: 
                os.remove(file_path)
                deleted_disk.append(file_name)
                logger.info(f"Deleted file: {file_path}")
            except OSError as e: 
                logger.error(f"Error deleting {file_path}: {e}")
        
        if os.path.exists(log_path):
            try: 
                os.remove(log_path)
                deleted_disk.append(os.path.basename(log_path))
                logger.info(f"Deleted log: {log_path}")
            except OSError as e: 
                logger.error(f"Error deleting log {log_path}: {e}")

        remove_user_file_db(script_owner_id, file_name)
        deleted_str = ", ".join(f"`{f}`" for f in deleted_disk) if deleted_disk else "associated files"
        
        try:
            bot.edit_message_text(
                f"🗑️ Record `{file_name}` (User `{script_owner_id}`) and {deleted_str} deleted!",
                chat_id_for_reply, call.message.message_id, reply_markup=None, parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Error editing message after delete: {e}")
            bot.send_message(chat_id_for_reply, f"🗑️ Record `{file_name}` deleted.", parse_mode='Markdown')
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing delete callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid delete command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in delete_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error deleting.", show_alert=True)

def logs_bot_callback(call):
    try:
        _, script_owner_id_str, file_name = call.data.split('_', 2)
        script_owner_id = int(script_owner_id_str)
        requesting_user_id = call.from_user.id
        chat_id_for_reply = call.message.chat.id

        logger.info(f"Logs: Requester={requesting_user_id}, Owner={script_owner_id}, File='{file_name}'")
        if not (requesting_user_id == script_owner_id or requesting_user_id in admin_ids):
            bot.answer_callback_query(call.id, "⚠️ Permission denied.", show_alert=True)
            return

        user_files_list = user_files.get(script_owner_id, [])
        if not any(f[0] == file_name for f in user_files_list):
            bot.answer_callback_query(call.id, "⚠️ File not found.", show_alert=True)
            check_files_callback(call)
            return

        user_folder = get_user_folder(script_owner_id)
        log_path = os.path.join(user_folder, f"{os.path.splitext(file_name)[0]}.log")
        
        if not os.path.exists(log_path):
            bot.answer_callback_query(call.id, f"⚠️ No logs for '{file_name}'.", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        
        try:
            log_content = ""
            file_size = os.path.getsize(log_path)
            max_log_kb = 100
            max_tg_msg = 4096
            
            if file_size == 0: 
                log_content = "(Log empty)"
            elif file_size > max_log_kb * 1024:
                 with open(log_path, 'rb') as f: 
                     f.seek(-max_log_kb * 1024, os.SEEK_END)
                     log_bytes = f.read()
                 log_content = log_bytes.decode('utf-8', errors='ignore')
                 log_content = f"(Last {max_log_kb} KB)\n...\n" + log_content
            else:
                 with open(log_path, 'r', encoding='utf-8', errors='ignore') as f: 
                     log_content = f.read()

            if len(log_content) > max_tg_msg:
                log_content = log_content[-max_tg_msg:]
                first_nl = log_content.find('\n')
                if first_nl != -1: 
                    log_content = "...\n" + log_content[first_nl+1:]
                else: 
                    log_content = "...\n" + log_content
            
            if not log_content.strip(): 
                log_content = "(No visible content)"

            bot.send_message(chat_id_for_reply, f"📜 Logs for `{file_name}` (User `{script_owner_id}`):\n```\n{log_content}\n```", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error reading/sending log {log_path}: {e}", exc_info=True)
            bot.send_message(chat_id_for_reply, f"❌ Error reading log for `{file_name}`.")
    except (ValueError, IndexError) as e:
        logger.error(f"Error parsing logs callback '{call.data}': {e}")
        bot.answer_callback_query(call.id, "Error: Invalid logs command.", show_alert=True)
    except Exception as e:
        logger.error(f"Error in logs_bot_callback for '{call.data}': {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Error fetching logs.", show_alert=True)

def speed_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    start_cb_ping_time = time.time()
    
    try:
        bot.edit_message_text("🏃 Testing speed...", chat_id, call.message.message_id)
        bot.send_chat_action(chat_id, 'typing')
        response_time = round((time.time() - start_cb_ping_time) * 1000, 2)
        status = "🔓 Unlocked" if not bot_locked else "🔒 Locked"
        
        if user_id == OWNER_ID: 
            user_level = "👑 Owner"
        elif user_id in admin_ids: 
            user_level = "🛡️ Admin"
        elif user_id in user_subscriptions and user_subscriptions[user_id].get('expiry', datetime.min) > datetime.now(): 
            user_level = "⭐ Premium"
        else: 
            user_level = "🆓 Free User"
        
        speed_msg = (f"⚡ Bot Speed & Status:\n\n⏱️ API Response Time: {response_time} ms\n"
                     f"🚦 Bot Status: {status}\n"
                     f"👤 Your Level: {user_level}")
        
        bot.answer_callback_query(call.id)
        bot.edit_message_text(speed_msg, chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
    except Exception as e:
         logger.error(f"Error during speed test (callback): {e}", exc_info=True)
         bot.answer_callback_query(call.id, "Error in speed test.", show_alert=True)
         try: 
             bot.edit_message_text("〽️ Main Menu", chat_id, call.message.message_id, reply_markup=create_main_menu_inline(user_id))
         except Exception: 
             pass

def back_to_main_callback(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    file_limit = get_user_file_limit(user_id)
    current_files = get_user_file_count(user_id)
    limit_str = str(file_limit) if file_limit != float('inf') else "Unlimited"
    expiry_info = ""
    
    if user_id == OWNER_ID: 
        user_status = "👑 Owner"
    elif user_id in admin_ids: 
        user_status = "🛡️ Admin"
    elif user_id in user_subscriptions:
        expiry_date = user_subscriptions[user_id].get('expiry')
        if expiry_date and expiry_date > datetime.now():
            user_status = "⭐ Premium"
            days_left = (expiry_date - datetime.now()).days
            expiry_info = f"\n⏳ Subscription expires in: {days_left} days"
        else: 
            user_status = "🆓 Free User (Expired Sub)"
    else: 
        user_status = "🆓 Free User"
    
    main_menu_text = (f"〽️ Welcome back, {call.from_user.first_name}!\n\n🆔 ID: `{user_id}`\n"
                      f"🔰 Status: {user_status}{expiry_info}\n📁 Files: {current_files} / {limit_str}\n\n"
                      f"👇 Use buttons or type commands.")
    
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text(main_menu_text, chat_id, call.message.message_id,
                              reply_markup=create_main_menu_inline(user_id), parse_mode='Markdown')
    except telebot.apihelper.ApiTelegramException as e:
         if "message is not modified" in str(e): 
             logger.warning("Message not modified (back_to_main).")
         else: 
             logger.error(f"API error on back_to_main: {e}")
    except Exception as e: 
        logger.error(f"Error handling back_to_main: {e}", exc_info=True)

# --- Admin Callback Implementations (for Inline Buttons) ---
def subscription_management_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("💳 Subscription Management\nSelect action:",
                              call.message.chat.id, call.message.message_id, reply_markup=create_subscription_menu())
    except Exception as e: 
        logger.error(f"Error showing subscription menu: {e}")

def stats_callback(call):
    bot.answer_callback_query(call.id)
    _logic_statistics(call.message)
    
    try:
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id,
                                      reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e:
        logger.error(f"Error updating menu after stats_callback: {e}")

def lock_bot_callback(call):
    global bot_locked
    bot_locked = True
    logger.warning(f"Bot locked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔒 Bot locked.")
    try: 
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 
                                      reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: 
        logger.error(f"Error updating menu (lock): {e}")

def unlock_bot_callback(call):
    global bot_locked
    bot_locked = False
    logger.warning(f"Bot unlocked by Admin {call.from_user.id}")
    bot.answer_callback_query(call.id, "🔓 Bot unlocked.")
    try: 
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, 
                                      reply_markup=create_main_menu_inline(call.from_user.id))
    except Exception as e: 
        logger.error(f"Error updating menu (unlock): {e}")

def run_all_scripts_callback(call):
    _logic_run_all_scripts(call)

def broadcast_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "📢 Send message to broadcast.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_broadcast_message)

def process_broadcast_message(message):
    user_id = message.from_user.id
    if user_id not in admin_ids: 
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    
    if message.text and message.text.lower() == '/cancel': 
        bot.reply_to(message, "Broadcast cancelled.")
        return

    broadcast_content = message.text
    if not broadcast_content and not (message.photo or message.video or message.document or message.sticker or message.voice or message.audio):
         bot.reply_to(message, "⚠️ Cannot broadcast empty message. Send text or media, or /cancel.")
         msg = bot.send_message(message.chat.id, "📢 Send broadcast message or /cancel.")
         bot.register_next_step_handler(msg, process_broadcast_message)
         return

    target_count = len(active_users)
    markup = types.InlineKeyboardMarkup()
    markup.row(types.InlineKeyboardButton("✅ Confirm & Send", callback_data=f"confirm_broadcast_{message.message_id}"),
               types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast"))

    preview_text = broadcast_content[:1000].strip() if broadcast_content else "(Media message)"
    bot.reply_to(message, f"⚠️ Confirm Broadcast:\n\n```\n{preview_text}\n```\n"
                          f"To **{target_count}** users. Sure?", reply_markup=markup, parse_mode='Markdown')

def handle_confirm_broadcast(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in admin_ids: 
        bot.answer_callback_query(call.id, "⚠️ Admin only.", show_alert=True)
        return
    
    try:
        original_message = call.message.reply_to_message
        if not original_message: 
            raise ValueError("Could not retrieve original message.")

        broadcast_text = None
        broadcast_photo_id = None
        broadcast_video_id = None

        if original_message.text:
            broadcast_text = original_message.text
        elif original_message.photo:
            broadcast_photo_id = original_message.photo[-1].file_id
        elif original_message.video:
            broadcast_video_id = original_message.video.file_id
        else:
            raise ValueError("Message has no text or supported media for broadcast.")

        bot.answer_callback_query(call.id, "🚀 Starting broadcast...")
        bot.edit_message_text(f"📢 Broadcasting to {len(active_users)} users...",
                              chat_id, call.message.message_id, reply_markup=None)
        
        thread = threading.Thread(target=execute_broadcast, args=(
            broadcast_text, broadcast_photo_id, broadcast_video_id,
            original_message.caption if (broadcast_photo_id or broadcast_video_id) else None,
            chat_id))
        thread.start()
    except ValueError as ve:
        logger.error(f"Error retrieving message for broadcast confirm: {ve}")
        bot.edit_message_text(f"❌ Error starting broadcast: {ve}", chat_id, call.message.message_id, reply_markup=None)
    except Exception as e:
        logger.error(f"Error in handle_confirm_broadcast: {e}", exc_info=True)
        bot.edit_message_text("❌ Unexpected error during broadcast confirm.", chat_id, call.message.message_id, reply_markup=None)

def handle_cancel_broadcast(call):
    bot.answer_callback_query(call.id, "Broadcast cancelled.")
    bot.delete_message(call.message.chat.id, call.message.message_id)
    
    if call.message.reply_to_message:
        try: 
            bot.delete_message(call.message.chat.id, call.message.reply_to_message.message_id)
        except: 
            pass

def execute_broadcast(broadcast_text, photo_id, video_id, caption, admin_chat_id):
    sent_count = 0
    failed_count = 0
    blocked_count = 0
    start_exec_time = time.time()
    users_to_broadcast = list(active_users)
    total_users = len(users_to_broadcast)
    
    logger.info(f"Executing broadcast to {total_users} users.")
    batch_size = 25
    delay_batches = 1.5

    for i, user_id_bc in enumerate(users_to_broadcast):
        try:
            if broadcast_text:
                bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
            elif photo_id:
                bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='Markdown' if caption else None)
            elif video_id:
                bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='Markdown' if caption else None)
            
            sent_count += 1
        except telebot.apihelper.ApiTelegramException as e:
            err_desc = str(e).lower()
            if any(s in err_desc for s in ["bot was blocked", "user is deactivated", "chat not found", "kicked from", "restricted"]):
                logger.warning(f"Broadcast failed to {user_id_bc}: User blocked/inactive.")
                blocked_count += 1
            elif "flood control" in err_desc or "too many requests" in err_desc:
                retry_after = 5
                match = re.search(r"retry after (\d+)", err_desc)
                if match: 
                    retry_after = int(match.group(1)) + 1
                
                logger.warning(f"Flood control. Sleeping {retry_after}s...")
                time.sleep(retry_after)
                
                try: # Retry once
                    if broadcast_text: 
                        bot.send_message(user_id_bc, broadcast_text, parse_mode='Markdown')
                    elif photo_id: 
                        bot.send_photo(user_id_bc, photo_id, caption=caption, parse_mode='Markdown' if caption else None)
                    elif video_id: 
                        bot.send_video(user_id_bc, video_id, caption=caption, parse_mode='Markdown' if caption else None)
                    
                    sent_count += 1
                except Exception as e_retry: 
                    logger.error(f"Broadcast retry failed to {user_id_bc}: {e_retry}")
                    failed_count += 1
            else: 
                logger.error(f"Broadcast failed to {user_id_bc}: {e}")
                failed_count += 1
        except Exception as e: 
            logger.error(f"Unexpected error broadcasting to {user_id_bc}: {e}")
            failed_count += 1

        if (i + 1) % batch_size == 0 and i < total_users - 1:
            logger.info(f"Broadcast batch {i//batch_size + 1} sent. Sleeping {delay_batches}s...")
            time.sleep(delay_batches)
        elif i % 5 == 0: 
            time.sleep(0.2)

    duration = round(time.time() - start_exec_time, 2)
    result_msg = (f"📢 Broadcast Complete!\n\n✅ Sent: {sent_count}\n❌ Failed: {failed_count}\n"
                  f"🚫 Blocked/Inactive: {blocked_count}\n👥 Targets: {total_users}\n⏱️ Duration: {duration}s")
    
    logger.info(result_msg)
    
    try: 
        bot.send_message(admin_chat_id, result_msg)
    except Exception as e: 
        logger.error(f"Failed to send broadcast result to admin {admin_chat_id}: {e}")

def admin_panel_callback(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("👑 Admin Panel\nManage admins (Owner actions may be restricted).",
                              call.message.chat.id, call.message.message_id, reply_markup=create_admin_panel())
    except Exception as e: 
        logger.error(f"Error showing admin panel: {e}")

def add_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID to promote to Admin.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_admin_id)

def process_add_admin_id(message):
    owner_id_check = message.from_user.id
    if owner_id_check != OWNER_ID: 
        bot.reply_to(message, "⚠️ Owner only.")
        return
    
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "Admin promotion cancelled.")
        return
    
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id <= 0: 
            raise ValueError("ID must be positive")
        
        if new_admin_id == OWNER_ID: 
            bot.reply_to(message, "⚠️ Owner is already Owner.")
            return
        
        if new_admin_id in admin_ids: 
            bot.reply_to(message, f"⚠️ User `{new_admin_id}` already Admin.")
            return
        
        add_admin_db(new_admin_id)
        logger.warning(f"Admin {new_admin_id} added by Owner {owner_id_check}.")
        bot.reply_to(message, f"✅ User `{new_admin_id}` promoted to Admin.")
        
        try: 
            bot.send_message(new_admin_id, "🎉 Congrats! You are now an Admin.")
        except Exception as e: 
            logger.error(f"Failed to notify new admin {new_admin_id}: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter User ID to promote or /cancel.")
        bot.register_next_step_handler(msg, process_add_admin_id)
    except Exception as e: 
        logger.error(f"Error processing add admin: {e}", exc_info=True)
        bot.reply_to(message, "Error.")

def remove_admin_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "👑 Enter User ID of Admin to remove.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_admin_id)

def process_remove_admin_id(message):
    owner_id_check = message.from_user.id
    if owner_id_check != OWNER_ID: 
        bot.reply_to(message, "⚠️ Owner only.")
        return
    
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "Admin removal cancelled.")
        return
    
    try:
        admin_id_remove = int(message.text.strip())
        if admin_id_remove <= 0: 
            raise ValueError("ID must be positive")
        
        if admin_id_remove == OWNER_ID: 
            bot.reply_to(message, "⚠️ Owner cannot remove self.")
            return
        
        if admin_id_remove not in admin_ids: 
            bot.reply_to(message, f"⚠️ User `{admin_id_remove}` not Admin.")
            return
        
        if remove_admin_db(admin_id_remove):
            logger.warning(f"Admin {admin_id_remove} removed by Owner {owner_id_check}.")
            bot.reply_to(message, f"✅ Admin `{admin_id_remove}` removed.")
            
            try: 
                bot.send_message(admin_id_remove, "ℹ️ You are no longer an Admin.")
            except Exception as e: 
                logger.error(f"Failed to notify removed admin {admin_id_remove}: {e}")
        else: 
            bot.reply_to(message, f"❌ Failed to remove admin `{admin_id_remove}`. Check logs.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "👑 Enter Admin ID to remove or /cancel.")
        bot.register_next_step_handler(msg, process_remove_admin_id)
    except Exception as e: 
        logger.error(f"Error processing remove admin: {e}", exc_info=True)
        bot.reply_to(message, "Error.")

def list_admins_callback(call):
    bot.answer_callback_query(call.id)
    try:
        admin_list_str = "\n".join(f"- `{aid}` {'(Owner)' if aid == OWNER_ID else ''}" for aid in sorted(list(admin_ids)))
        if not admin_list_str: 
            admin_list_str = "(No Owner/Admins configured!)"
        
        bot.edit_message_text(f"👑 Current Admins:\n\n{admin_list_str}", call.message.chat.id,
                              call.message.message_id, reply_markup=create_admin_panel(), parse_mode='Markdown')
    except Exception as e: 
        logger.error(f"Error listing admins: {e}")

def add_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID & days (e.g., `12345678 30`).\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_add_subscription_details)

def process_add_subscription_details(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: 
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "Subscription addition cancelled.")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 2: 
            raise ValueError("Incorrect format")
        
        sub_user_id = int(parts[0].strip())
        days = int(parts[1].strip())
        
        if sub_user_id <= 0 or days <= 0: 
            raise ValueError("User ID/days must be positive")

        current_expiry = user_subscriptions.get(sub_user_id, {}).get('expiry')
        start_date_new_sub = datetime.now()
        
        if current_expiry and current_expiry > start_date_new_sub: 
            start_date_new_sub = current_expiry
        
        new_expiry = start_date_new_sub + timedelta(days=days)
        save_subscription(sub_user_id, new_expiry)

        logger.info(f"Subscription for {sub_user_id} by admin {admin_id_check}. Expiry: {new_expiry:%Y-%m-%d}")
        bot.reply_to(message, f"✅ Subscription for `{sub_user_id}` by {days} days.\nNew expiry: {new_expiry:%Y-%m-%d}")
        
        try: 
            bot.send_message(sub_user_id, f"🎉 Subscription activated/extended by {days} days! Expires: {new_expiry:%Y-%m-%d}.")
        except Exception as e: 
            logger.error(f"Failed to notify {sub_user_id} of new subscription: {e}")
    except ValueError as e:
        bot.reply_to(message, f"⚠️ Invalid: {e}. Format: `ID days` or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID & days, or /cancel.")
        bot.register_next_step_handler(msg, process_add_subscription_details)
    except Exception as e: 
        logger.error(f"Error processing add subscription: {e}", exc_info=True)
        bot.reply_to(message, "Error.")

def remove_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to remove subscription.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_remove_subscription_id)

def process_remove_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: 
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "Subscription removal cancelled.")
        return
    
    try:
        sub_user_id_remove = int(message.text.strip())
        if sub_user_id_remove <= 0: 
            raise ValueError("ID must be positive")
        
        if sub_user_id_remove not in user_subscriptions:
            bot.reply_to(message, f"⚠️ User `{sub_user_id_remove}` no active subscription in memory.")
            return
        
        remove_subscription_db(sub_user_id_remove)
        logger.warning(f"Subscription removed for {sub_user_id_remove} by admin {admin_id_check}.")
        bot.reply_to(message, f"✅ Subscription for `{sub_user_id_remove}` removed.")
        
        try: 
            bot.send_message(sub_user_id_remove, "ℹ️ Your subscription removed by admin.")
        except Exception as e: 
            logger.error(f"Failed to notify {sub_user_id_remove} of subscription removal: {e}")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to remove subscription from, or /cancel.")
        bot.register_next_step_handler(msg, process_remove_subscription_id)
    except Exception as e: 
        logger.error(f"Error processing remove subscription: {e}", exc_info=True)
        bot.reply_to(message, "Error.")

def check_subscription_init_callback(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "💳 Enter User ID to check subscription.\n/cancel to abort.")
    bot.register_next_step_handler(msg, process_check_subscription_id)

def process_check_subscription_id(message):
    admin_id_check = message.from_user.id
    if admin_id_check not in admin_ids: 
        bot.reply_to(message, "⚠️ Not authorized.")
        return
    
    if message.text.lower() == '/cancel': 
        bot.reply_to(message, "Subscription check cancelled.")
        return
    
    try:
        sub_user_id_check = int(message.text.strip())
        if sub_user_id_check <= 0: 
            raise ValueError("ID must be positive")
        
        if sub_user_id_check in user_subscriptions:
            expiry_dt = user_subscriptions[sub_user_id_check].get('expiry')
            if expiry_dt:
                if expiry_dt > datetime.now():
                    days_left = (expiry_dt - datetime.now()).days
                    bot.reply_to(message, f"✅ User `{sub_user_id_check}` active subscription.\nExpires: {expiry_dt:%Y-%m-%d %H:%M:%S} ({days_left} days left).")
                else:
                    bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` expired subscription (On: {expiry_dt:%Y-%m-%d %H:%M:%S}).")
                    remove_subscription_db(sub_user_id_check) # Clean up
            else: 
                bot.reply_to(message, f"⚠️ User `{sub_user_id_check}` in subscription list, but expiry missing. Re-add if needed.")
        else: 
            bot.reply_to(message, f"ℹ️ User `{sub_user_id_check}` no active subscription record.")
    except ValueError:
        bot.reply_to(message, "⚠️ Invalid ID. Send numerical ID or /cancel.")
        msg = bot.send_message(message.chat.id, "💳 Enter User ID to check, or /cancel.")
        bot.register_next_step_handler(msg, process_check_subscription_id)
    except Exception as e: 
        logger.error(f"Error processing check subscription: {e}", exc_info=True)
        bot.reply_to(message, "Error.")

# --- End Callback Query Handlers ---

# --- Cleanup Function ---
def cleanup():
    logger.warning("Shutdown. Cleaning up processes...")
    script_keys_to_stop = list(bot_scripts.keys())
    if not script_keys_to_stop: 
        logger.info("No scripts running. Exiting.")
        return
    
    logger.info(f"Stopping {len(script_keys_to_stop)} scripts...")
    for key in script_keys_to_stop:
        if key in bot_scripts: 
            logger.info(f"Stopping: {key}")
            kill_process_tree(bot_scripts[key])
        else: 
            logger.info(f"Script {key} already removed.")
    
    logger.warning("Cleanup finished.")

atexit.register(cleanup)

# --- Main Execution ---
if __name__ == '__main__':
    logger.info("="*40 + "\n🤖 Bot Starting Up...\n" + f"🐍 Python: {sys.version.split()[0]}\n" +
                f"🔧 Base Dir: {BASE_DIR}\n📁 Upload Dir: {UPLOAD_BOTS_DIR}\n" +
                f"📊 Data Dir: {IROTECH_DIR}\n🔑 Owner ID: {OWNER_ID}\n🛡️ Admins: {admin_ids}\n" + "="*40)
    
    keep_alive()
    logger.info("🚀 Starting polling...")
    
    while True:
        try:
            bot.infinity_polling(logger_level=logging.INFO, timeout=60, long_polling_timeout=30)
        except requests.exceptions.ReadTimeout: 
            logger.warning("Polling ReadTimeout. Restarting in 5s...")
            time.sleep(5)
        except requests.exceptions.ConnectionError as ce: 
            logger.error(f"Polling ConnectionError: {ce}. Retrying in 15s...")
            time.sleep(15)
        except Exception as e:
            logger.critical(f"💥 Unrecoverable polling error: {e}", exc_info=True)
            logger.info("Restarting polling in 30s due to critical error...")
            time.sleep(30)
        finally: 
            logger.warning("Polling attempt finished. Will restart if in loop.")
            time.sleep(1)
