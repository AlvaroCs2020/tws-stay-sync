import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
from datetime import datetime


def format_int_to_string(num):
    def fmt(n):
        return f"{n:.1f}".rstrip("0").rstrip(".")

    abs_num = abs(num)
    sign = "-" if num < 0 else ""
    if abs_num >= 1_000_000_000:
        return f"{sign}{fmt(abs_num / 1_000_000_000)} B"
    elif abs_num >= 1_000_000:
        return f"{sign}{fmt(abs_num / 1_000_000)} M"
    elif abs_num >= 1_000:
        return f"{sign}{fmt(abs_num / 1_000)} K"
    else:
        return f"{num}"

symbol_id = int(input("-Ingresa 1 para traer datos de EURUSD o 7 para datos de XAUUSD (luego presionar ENTER): "))
while symbol_id not in [1,7]:
    print("+Me estas pidiendo una divisa que todavia no esta cargada!!! ingresa 7 o 1")
    symbol_id = int(input("-Ingresa 1 para traer datos de EURUSD o 7 para datos de XAUUSD: "))

limit_date = int(input("- la data en un rago de fechas especifico? De ser asi ingresa 1 (luego presionar ENTER):"))

date_from = '2025-06-31 00:00:00'
date_to = datetime.today()
place_holder = ""
if limit_date == 1:
    print("+Vamos a acotar la informacion en el rango de fechas que necesitas, necesito que ingreses cuidadosamente el formato YYYY-MM-DD HH:MM:SS, EJEMPLO: 2025-06-31 00:00:00")
    date_from = str(input("-Ingresa la fecha de inicio para tu analisis: "))
    place_holder = str(input("-La fecha de fin para tu analisis SI QUERES HASTA HOY, SOLO TOCA ENTER: "))
    print("+Okay, ahora el programa va a buscar data entre estas fechas, si las ingresaste mal vas a ver un error, cerra el programa e intenta de nuevo.")
if place_holder != "":
    date_to = place_holder
# --- Conexión y datos

host = "aws-0-us-east-2.pooler.supabase.com"
conn = psycopg2.connect(
    host=host,
    port=6543,
    user="postgres.ikdkhversotaizbhvwyh",
    password="Asdqwerty_09",
    dbname="postgres"
)
sql = f""" SELECT * FROM "LIQUIDEZTEST" WHERE "symbol_id" = {symbol_id} AND "date_id" <= TIMESTAMP WITH TIME ZONE '{date_to}' AND "date_id" >= TIMESTAMP WITH TIME ZONE '{date_from}'  ORDER BY "date_id" DESC """
print("Vamos a buscar la informacion a la DB. Cargando...")
with conn.cursor() as cur:
    cur.execute(sql)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
df = pd.DataFrame(rows, columns=colnames)

# --- Preprocesamiento
df_sorted = df.sort_values("date_id").reset_index(drop=True)
df_sorted["date_id"] = pd.to_datetime(df_sorted["date_id"])
count_tick = df_sorted["count_tick"].sum()
min_time = df_sorted["date_id"].min()
max_time = df_sorted["date_id"].max()

x = df_sorted.index
y = df_sorted["price_bid"]
x_date = df_sorted["date_id"]
mask = ~df_sorted["boolean"].astype(bool)

# --- Crear figura y eje
fig, ax = plt.subplots(figsize=(15, 6))
fig.patch.set_facecolor('black')
ax.set_facecolor('black')

# --- Línea azul Precio Bid
ax.plot(x, y, label="Precio Bid", color="blue", zorder=1)

# --- Puntos rojos
ax.scatter(x[mask], y[mask], color="red", marker="o", s=5, label="Liquidez sin saldar", zorder=3)

# --- Líneas horizontales con difference
for xi in x[mask]:
    yi = y[xi]
    diff = df_sorted.loc[xi, "difference"]
    color = "red" if diff > 0 else "green"
    ax.hlines(yi, xi, x[-1] + 50, colors=color, linewidth=1.5, zorder=2)

# --- Líneas verticales por cambio de día
previous_day = None
tick_positions = []
tick_labels = []

for i, ts in enumerate(x_date):
    current_day = ts.date()
    if current_day != previous_day:
        ax.axvline(x=i, color="white", linestyle="--", linewidth=1.5)
        tick_positions.append(i)
        tick_labels.append(ts.strftime("%d-%b"))
        previous_day = current_day

ax.set_xticks(tick_positions)
ax.set_xticklabels(tick_labels, rotation=45, color="white")

# --- Estética
ax.set_xlabel("Fecha", color="white")
ax.set_ylabel("Price Bid", color="white")
ax.set_title(
    f"Curva Price Bid - sample: MAX 1seg - {min_time} to {max_time} - ticks: {count_tick} - registros: {len(df_sorted)}",
    color="white"
)
ax.grid(True, color="gray", linestyle="--")
ax.tick_params(colors='white')
ax.legend()

# --- Herramientas
toolbar = plt.get_current_fig_manager().toolbar  # obtener toolbar real

# --- Interacción
clicked_line_h_r = None
clicked_line_v_r = None
clicked_text_r = None

clicked_line_h_l = None
clicked_line_v_l = None
clicked_text_l = None

x_l = 0
y_l = 0

x_r = 0
y_r = 0

text_sum = None

def onclick(event):
    global clicked_line_h_r, clicked_line_v_r, clicked_text_r
    global clicked_line_h_l, clicked_line_v_l, clicked_text_l
    global x_l, y_l, x_r, y_r
    global text_sum
    if toolbar.mode != '':
        return  # evitar si pan/zoom activos

    if event.inaxes != ax:
        return

    x_pos = int(round(event.xdata))
    y_val = event.ydata

    if x_pos < 0 or x_pos >= len(df_sorted):
        return

    date_str = df_sorted.loc[x_pos, "date_id"].strftime("%Y-%m-%d %H:%M:%S")
    text = f"{y_val:.5f}\n{date_str}"

    if event.button == 3:  # clic derecho
        if clicked_line_h_r: clicked_line_h_r.remove()
        if clicked_line_v_r: clicked_line_v_r.remove()
        if clicked_text_r: clicked_text_r.remove()

        clicked_line_h_r = ax.axhline(y_val, color='yellow', linestyle='dotted', linewidth=1.5)
        clicked_line_v_r = ax.axvline(x=x_pos, color="yellow", linestyle="dotted", linewidth=1.5)
        clicked_text_r = ax.text(0, y_val, text, color='yellow', fontsize=10,
                                 verticalalignment='bottom', horizontalalignment='left',
                                 bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor="yellow"))
        y_r = y_val
    elif event.button == 1:  # clic izquierdo
        if clicked_line_h_l: clicked_line_h_l.remove()
        if clicked_line_v_l: clicked_line_v_l.remove()
        if clicked_text_l: clicked_text_l.remove()

        clicked_line_h_l = ax.axhline(y_val, color='yellow', linestyle='dotted', linewidth=1.5)
        clicked_line_v_l = ax.axvline(x=x_pos, color="yellow", linestyle="dotted", linewidth=1.5)
        clicked_text_l = ax.text(0, y_val, text, color='yellow', fontsize=10,
                                 verticalalignment='bottom', horizontalalignment='left',
                                 bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor="yellow"))
        y_l = y_val

    elif event.button == 2:  # clic del medio
        for artist in [clicked_line_h_r, clicked_line_v_r, clicked_text_r,
                       clicked_line_h_l, clicked_line_v_l, clicked_text_l, text_sum]:
            try:
                if artist:
                    artist.remove()
            except ValueError:
                pass  # ya fue eliminado
        clicked_line_h_r = clicked_line_v_r = clicked_text_r = None
        clicked_line_h_l = clicked_line_v_l = clicked_text_l = None
    fig.canvas.draw()


def onkey(event):
    global clicked_line_h_r, clicked_line_v_r, clicked_text_r
    global clicked_line_h_l, clicked_line_v_l, clicked_text_l
    global x_l, y_l, x_r, y_r
    global text_sum
    if event.key.lower() == 'c':
        if text_sum:
            text_sum.remove()
        if clicked_line_h_l and clicked_line_h_r:
            y_min = min(y_l, y_r)
            y_max = max(y_l, y_r)
            y_text = (y_max - y_min)/2 + y_min
            df_filtered = df_sorted[(df_sorted["price_bid"] >= y_min) & (df_sorted["price_bid"] <= y_max) & (df_sorted["boolean"] == False)]
            diff_sum = df_filtered["difference"].sum()
            color = "red" if diff_sum > 0 else "green"

            text_sum = ax.text((len(df_sorted) + 200), y_text, format_int_to_string(diff_sum), color=color, fontsize=10,
                    verticalalignment='center', horizontalalignment='center',
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor=color))

            print(f"✅ Suma de 'difference' entre {y_min:.5f} y {y_max:.5f} = {diff_sum:.5f}")
        else:
            print("⚠️ Debés tener las dos líneas horizontales marcadas para calcular.")
        fig.canvas.draw()


fig.canvas.mpl_connect("button_press_event", onclick)
fig.canvas.mpl_connect("key_press_event", onkey)

plt.tight_layout()
instructions = """
Instrucciones:
Para marcar lineas te tenes que fijar que no esten seleccionas las herramientas de navegacion!! (son los botones abajo a la izquierda)
Podes marcar lineas con el click derecho e izquierdo, cada click controla una linea
Con dos lineas marcadas, podes obtener toda la liquidez, que no esta saldada entre ambas lineas precionando la tecla 'C'
Podes tambien observar el valor completo en la consola
"""
print(instructions)

plt.show()

# eurusd #abril
# gbpusd #junio
# xausd #abril
