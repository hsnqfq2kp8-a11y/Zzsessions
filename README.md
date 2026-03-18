# ============================================
# اعداداتك - غير هنا فقط:
TOKEN = "ضع_توكن_البوت_هنا"  # من @BotFather
ADMIN = 123456789              # من @userinfobot
# ============================================

import os, sqlite3, asyncio, logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ConversationHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)

# قاعدة البيانات
class DB:
    def __init__(self):
        self.conn = sqlite3.connect('data.db', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS slots (id INTEGER PRIMARY KEY, date TEXT, time TEXT, booked INTEGER DEFAULT 0, UNIQUE(date, time))")
        c.execute("CREATE TABLE IF NOT EXISTS bookings (id INTEGER PRIMARY KEY, slot_id INTEGER, user_id INTEGER, name TEXT, phone TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS reminders (booking_id INTEGER, type TEXT, sent INTEGER DEFAULT 0)")
        self.conn.commit()
    
    def add(self, d, t):
        try:
            c = self.conn.cursor()
            c.execute("INSERT INTO slots (date, time) VALUES (?, ?)", (d, t))
            self.conn.commit()
            return True
        except:
            return False
    
    def available(self):
        c = self.conn.cursor()
        today = datetime.now().date().isoformat()
        c.execute("SELECT id, date, time FROM slots WHERE booked=0 AND date>=? ORDER BY date, time", (today,))
        return c.fetchall()
    
    def by_date(self, d):
        c = self.conn.cursor()
        c.execute("SELECT id, time FROM slots WHERE date=? AND booked=0 ORDER BY time", (d,))
        return c.fetchall()
    
    def book(self, sid, uid, name, phone):
        c = self.conn.cursor()
        c.execute("SELECT booked FROM slots WHERE id=?", (sid,))
        r = c.fetchone()
        if not r or r['booked']:
            return None
        c.execute("UPDATE slots SET booked=1 WHERE id=?", (sid,))
        c.execute("INSERT INTO bookings (slot_id, user_id, name, phone) VALUES (?, ?, ?, ?)", (sid, uid, name, phone))
        self.conn.commit()
        return c.lastrowid
    
    def my(self, uid):
        c = self.conn.cursor()
        c.execute("SELECT b.id, s.date, s.time FROM bookings b JOIN slots s ON b.slot_id=s.id WHERE b.user_id=? AND s.date>=date('now') ORDER BY s.date", (uid,))
        return c.fetchall()
    
    def cancel(self, bid, uid):
        c = self.conn.cursor()
        c.execute("SELECT slot_id FROM bookings WHERE id=? AND user_id=?", (bid, uid))
        r = c.fetchone()
        if not r:
            return False
        c.execute("UPDATE slots SET booked=0 WHERE id=?", (r['slot_id'],))
        c.execute("DELETE FROM bookings WHERE id=?", (bid,))
        self.conn.commit()
        return True
    
    def all(self):
        c = self.conn.cursor()
        c.execute("SELECT b.id, b.name, b.phone, s.date, s.time FROM bookings b JOIN slots s ON b.slot_id=s.id WHERE s.date>=date('now')")
        return c.fetchall()

db = DB()
D, T, P, C = range(4)

# التذكيرات
async def reminder(app):
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        c = db.conn.cursor()
        c.execute("SELECT b.id, b.user_id, s.date, s.time FROM bookings b JOIN slots s ON b.slot_id=s.id")
        for r in c.fetchall():
            apt = datetime.strptime(f"{r['date']} {r['time']}", "%Y-%m-%d %H:%M")
            m = (apt - now).total_seconds() / 60
            if 1380 <= m <= 1440:
                cc = db.conn.cursor()
                cc.execute("SELECT 1 FROM reminders WHERE booking_id=? AND type='24h'", (r['id'],))
                if not cc.fetchone():
                    await app.bot.send_message(r['user_id'], f"تذكير: موعدك غدا {r['date']} الساعة {r['time']}")
                    await app.bot.send_message(ADMIN, f"تذكير: موعد غدا للعميل {r['user_id']}")
                    cc.execute("INSERT INTO reminders VALUES (?, '24h', 1)", (r['id'],))
                    db.conn.commit()
            elif 50 <= m <= 60:
                cc = db.conn.cursor()
                cc.execute("SELECT 1 FROM reminders WHERE booking_id=? AND type='1h'", (r['id'],))
                if not cc.fetchone():
                    await app.bot.send_message(r['user_id'], f"تذكير عاجل: موعدك بعد ساعة ({r['time']})")
                    await app.bot.send_message(ADMIN, f"تذكير عاجل: موعد بعد ساعة للعميل {r['user_id']}")
                    cc.execute("INSERT INTO reminders VALUES (?, '1h', 1)", (r['id'],))
                    db.conn.commit()

# البوت
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("مرحبا بك في نظام حجز الجلسات النفسية\n\nاختر من القائمة:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("حجز موعد جديد", callback_data='b')],
        [InlineKeyboardButton("عرض حجوزاتي", callback_data='m')],
        [InlineKeyboardButton("الغاء حجز", callback_data='c')],
        [InlineKeyboardButton("تواصل معنا", callback_data='con')]
    ]))

async def book(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    s = db.available()
    if not s:
        await q.edit_message_text("لا توجد مواعيد متاحة حاليا")
        return -1
    d = {}
    for x in s:
        if x['date'] not in d:
            d[x['date']] = []
        d[x['date']].append((x['id'], x['time']))
    kb = [[InlineKeyboardButton(dd, callback_data=f"d_{dd}")] for dd in sorted(d.keys())]
    kb.append([InlineKeyboardButton("رجوع", callback_data='menu')])
    await q.edit_message_text("المواعيد المتاحة - اختر التاريخ:", reply_markup=InlineKeyboardMarkup(kb))
    return D

async def dsel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    dd = q.data[2:]
    context.user_data['date'] = dd
    tt = db.by_date(dd)
    kb = [[InlineKeyboardButton(t['time'], callback_data=f"t_{t['id']}_{t['time']}")] for t in tt]
    kb.append([InlineKeyboardButton("رجوع", callback_data='b')])
    await q.edit_message_text(f"التاريخ المختار: {dd}\nاختر الوقت المناسب:", reply_markup=InlineKeyboardMarkup(kb))
    return T

async def tsel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, sid, tm = q.data.split('_')
    context.user_data['slot'], context.user_data['time'] = int(sid), tm
    await q.edit_message_text("ادخل رقم هاتفك للتواصل:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("رجوع", callback_data='menu')]]))
    return P

async def pget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ph = update.message.text.strip()
    if not ph.isdigit() or len(ph) < 8:
        await update.message.reply_text("رقم غير صحيح، حاول مرة اخرى:")
        return P
    context.user_data['phone'] = ph
    d, t = context.user_data['date'], context.user_data['time']
    await update.message.reply_text(f"تفاصيل الحجز:\nالتاريخ: {d}\nالوقت: {t}\nالهاتف: {ph}\n\nتأكيد الحجز؟", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("تأكيد", callback_data='ok')],
        [InlineKeyboardButton("الغاء", callback_data='menu')]
    ]))
    return C

async def cfm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = update.effective_user
    bid = db.book(context.user_data['slot'], u.id, u.first_name, context.user_data['phone'])
    if bid:
        await q.edit_message_text(f"تم تأكيد الحجز بنجاح\nرقم الحجز: {bid}\n\nسيتم ارسال تذكير قبل الموعد", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("القائمة الرئيسية", callback_data='menu')]]))
        await context.bot.send_message(ADMIN, f"حجز جديد رقم {bid} من {u.first_name}")
    else:
        await q.edit_message_text("عذرا، هذا الموعد لم يعد متاح", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("رجوع", callback_data='b')]]))
    return -1

async def my(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    b = db.my(update.effective_user.id)
    if not b:
        await q.edit_message_text("لا توجد حجوزات حالية", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("حجز موعد", callback_data='b')], [InlineKeyboardButton("رجوع", callback_data='menu')]]))
        return
    txt, kb = "حجوزاتك القادمة:\n\n", []
    for x in b:
        txt += f"رقم {x['id']} - {x['date']} - الساعة {x['time']}\n"
        kb.append([InlineKeyboardButton(f"الغاء حجز رقم {x['id']}", callback_data=f"x_{x['id']}")])
    kb.append([InlineKeyboardButton("رجوع", callback_data='menu')])
    await q.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(kb))

async def can(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data.startswith('x_'):
        bid = int(q.data[2:])
        if db.cancel(bid, update.effective_user.id):
            await q.edit_message_text("تم الغاء الحجز بنجاح", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("القائمة", callback_data='menu')]]))
            await context.bot.send_message(ADMIN, f"تم الغاء الحجز رقم {bid}")
        else:
            await q.edit_message_text("لم يتم العثور على الحجز", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("رجوع", callback_data='menu')]]))
    else:
        await my(update, context)

async def con(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("للتواصل المباشر مع المعالج", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("رجوع", callback_data='menu')]]))

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await start(update, context)
    return -1

async def add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN:
        await update.message.reply_text("غير مصرح")
        return
    if len(context.args) < 2:
        await update.message.reply_text("الاستخدام: /add 2026-03-20 09:00,10:00,14:00")
        return
    d, ts = context.args[0], context.args[1].split(',')
    n = sum(1 for t in ts if db.add(d, t.strip()))
    await update.message.reply_text(f"تم اضافة {n} مواعيد")

async def allb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN:
        await update.message.reply_text("غير مصرح")
        return
    b = db.all()
    if not b:
        await update.message.reply_text("لا توجد حجوزات")
        return
    t = "قائمة الحجوزات:\n\n"
    for x in b:
        t += f"رقم {x['id']} - {x['name']} - {x['date']} الساعة {x['time']}\n"
    await update.message.reply_text(t)

def main():
    if TOKEN == "ضع_توكن_البوت_هنا":
        print("!!! عدل TOKEN في اول الكود !!!")
        return
    app = Application.builder().token(TOKEN).build()
    cv = ConversationHandler(entry_points=[CallbackQueryHandler(book, pattern='^b$')], states={D: [CallbackQueryHandler(dsel, pattern='^d_')], T: [CallbackQueryHandler(tsel, pattern='^t_')], P: [MessageHandler(filters.TEXT & ~filters.COMMAND, pget)], C: [CallbackQueryHandler(cfm, pattern='^ok$')]}, fallbacks=[CallbackQueryHandler(menu, pattern='^menu$')])
    app.add_handler(CommandHandler('start', start))
    app.add_handler(cv)
    app.add_handler(CallbackQueryHandler(my, pattern='^m$'))
    app.add_handler(CallbackQueryHandler(can, pattern='^c$|^x_'))
    app.add_handler(CallbackQueryHandler(con, pattern='^con$'))
    app.add_handler(CallbackQueryHandler(menu, pattern='^menu$'))
    app.add_handler(CommandHandler('add', add))
    app.add_handler(CommandHandler('all', allb))
    asyncio.create_task(reminder(app))
    print("البوت يعمل...")
    app.run_polling()

if __name__ == '__main__':
    main()

