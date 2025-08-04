import tkinter as tk
from tkinter import ttk
from datetime import datetime
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

# === Función para formatear diferencias ===
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

# === UI inicial ===
root = tk.Tk()
root.title("Liquidez Visualizer")
root.configure(bg="black")

frame_inputs = tk.Frame(root, bg="black")
frame_inputs.pack(padx=10, pady=10, fill="x")

symbol_var = tk.StringVar()
from_var = tk.StringVar()
to_var = tk.StringVar()

tk.Label(frame_inputs, text="Symbol ID (1=EURUSD, 3=GBPUSD, 7=XAUUSD):", bg="black", fg="white").grid(row=0, column=0, padx=5)
tk.Entry(frame_inputs, textvariable=symbol_var, width=5).grid(row=0, column=1, padx=5)

tk.Label(frame_inputs, text="Desde (YYYY-MM-DD HH:MM:SS):", bg="black", fg="white").grid(row=0, column=2, padx=5)
tk.Entry(frame_inputs, textvariable=from_var, width=20).grid(row=0, column=3, padx=5)

tk.Label(frame_inputs, text="Hasta (vacío = ahora):", bg="black", fg="white").grid(row=0, column=4, padx=5)
tk.Entry(frame_inputs, textvariable=to_var, width=20).grid(row=0, column=5, padx=5)

frame_plot = tk.Frame(root, bg="black")
frame_plot.pack(fill="both", expand=True)

clicked_line_h_r = clicked_line_v_r = clicked_text_r = None
clicked_line_h_l = clicked_line_v_l = clicked_text_l = None
x_l = y_l = x_r = y_r = 0
text_sum = None

# === Función de recarga principal ===
def recargar():
    global clicked_line_h_r, clicked_line_v_r, clicked_text_r
    global clicked_line_h_l, clicked_line_v_l, clicked_text_l
    global x_l, y_l, x_r, y_r, text_sum

    for widget in frame_plot.winfo_children():
        widget.destroy()

    try:
        symbol_id = int(symbol_var.get())
        if symbol_id not in [1, 3, 7]:
            raise ValueError("ID inválido")

        date_from = from_var.get()
        if not date_from:
            raise ValueError("Fecha desde requerida")
        date_to = to_var.get()
        if date_to == "":
            date_to = datetime.today()

        # === DB
        conn = psycopg2.connect(
            host="aws-0-us-east-2.pooler.supabase.com",
            port=6543,
            user="postgres.ikdkhversotaizbhvwyh",
            password="Asdqwerty_09",
            dbname="postgres"
        )
        sql = f"""
        SELECT * FROM "LIQUIDEZTEST" 
        WHERE "symbol_id" = {symbol_id}
        AND "date_id" BETWEEN TIMESTAMP WITH TIME ZONE '{date_from}' AND TIMESTAMP WITH TIME ZONE '{date_to}'
        ORDER BY "date_id" DESC
        """
        print("Buscando info...")
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        conn.close()

        # === Preprocesamiento
        df_sorted = df.sort_values("date_id").reset_index(drop=True)
        df_sorted["date_id"] = pd.to_datetime(df_sorted["date_id"])
        count_tick = df_sorted["count_tick"].sum()
        min_time = df_sorted["date_id"].min()
        max_time = df_sorted["date_id"].max()

        x = df_sorted.index
        y = df_sorted["price_bid"]
        x_date = df_sorted["date_id"]
        mask = ~df_sorted["boolean"].astype(bool)

        # === Plot
        fig, ax = plt.subplots(figsize=(15, 6))
        fig.patch.set_facecolor('black')
        ax.set_facecolor('black')

        ax.plot(x, y, label="Precio Bid", color="blue", zorder=1)
        ax.scatter(x[mask], y[mask], color="red", marker="o", s=5, label="Liquidez sin saldar", zorder=3)

        for xi in x[mask]:
            yi = y[xi]
            diff = df_sorted.loc[xi, "difference"]
            color = "red" if diff > 0 else "green"
            ax.hlines(yi, xi, x[-1] + 50, colors=color, linewidth=1.5, zorder=2)

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
        ax.set_xlabel("Fecha", color="white")
        ax.set_ylabel("Price Bid", color="white")
        ax.set_title(
            f"Curva Price Bid - sample: MAX 1seg - {min_time} to {max_time} - ticks: {count_tick} - registros: {len(df_sorted)}",
            color="white"
        )
        ax.grid(True, color="gray", linestyle="--")
        ax.tick_params(colors='white')
        ax.legend()

        # === Interacción
        def onclick(event):
            global clicked_line_h_r, clicked_line_v_r, clicked_text_r
            global clicked_line_h_l, clicked_line_v_l, clicked_text_l
            global x_l, y_l, x_r, y_r, text_sum

            if event.inaxes != ax:
                return
            try:
                x_pos = int(round(event.xdata))
                y_val = event.ydata
                if x_pos < 0 or x_pos >= len(df_sorted):
                    return
                date_str = df_sorted.loc[x_pos, "date_id"].strftime("%Y-%m-%d %H:%M:%S")
                text = f"{y_val:.5f}\n{date_str}"
                if event.button == 3:
                    for line in [clicked_line_h_r, clicked_line_v_r, clicked_text_r]:
                        if line: line.remove()
                    clicked_line_h_r = ax.axhline(y_val, color='yellow', linestyle='dotted', linewidth=1.5)
                    clicked_line_v_r = ax.axvline(x=x_pos, color="yellow", linestyle="dotted", linewidth=1.5)
                    clicked_text_r = ax.text(0, y_val, text, color='yellow', fontsize=10,
                        verticalalignment='bottom', horizontalalignment='left',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor="yellow"))
                    y_r = y_val
                elif event.button == 1:
                    for line in [clicked_line_h_l, clicked_line_v_l, clicked_text_l]:
                        if line: line.remove()
                    clicked_line_h_l = ax.axhline(y_val, color='yellow', linestyle='dotted', linewidth=1.5)
                    clicked_line_v_l = ax.axvline(x=x_pos, color="yellow", linestyle="dotted", linewidth=1.5)
                    clicked_text_l = ax.text(0, y_val, text, color='yellow', fontsize=10,
                        verticalalignment='bottom', horizontalalignment='left',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor="yellow"))
                    y_l = y_val
                elif event.button == 2:
                    for artist in [clicked_line_h_r, clicked_line_v_r, clicked_text_r,
                                   clicked_line_h_l, clicked_line_v_l, clicked_text_l, text_sum]:
                        try:
                            if artist:
                                artist.remove()
                        except: pass
                    clicked_line_h_r = clicked_line_v_r = clicked_text_r = None
                    clicked_line_h_l = clicked_line_v_l = clicked_text_l = None
                    text_sum = None
                fig.canvas.draw()
                canvas.get_tk_widget().pack(fill="both", expand=True)

                # --- Agregar barra de herramientas
                toolbar = NavigationToolbar2Tk(canvas, frame_plot)
                toolbar.update()
                toolbar.pack(side=tk.TOP, fill=tk.X)
            except:
                pass

        def onkey(event):
            global text_sum
            if event.key.lower() == 'c':
                if text_sum:
                    text_sum.remove()
                if clicked_line_h_l and clicked_line_h_r:
                    y_min = min(y_l, y_r)
                    y_max = max(y_l, y_r)
                    y_text = (y_max - y_min) / 2 + y_min
                    df_filtered = df_sorted[
                        (df_sorted["price_bid"] >= y_min) &
                        (df_sorted["price_bid"] <= y_max) &
                        (df_sorted["boolean"] == False)
                    ]
                    diff_sum = df_filtered["difference"].sum()
                    color = "red" if diff_sum > 0 else "green"
                    text_sum = ax.text((len(df_sorted) + 200), y_text, format_int_to_string(diff_sum), color=color,
                        fontsize=10, verticalalignment='center', horizontalalignment='center',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="black", edgecolor=color))
                    print(f"✅ Suma de 'difference' entre {y_min:.5f} y {y_max:.5f} = {diff_sum:.5f}")
                    fig.canvas.draw()
                else:
                    print("⚠️ Tenés que marcar dos líneas horizontales (clic izq y der)")

        fig.canvas.mpl_connect("button_press_event", onclick)
        fig.canvas.mpl_connect("key_press_event", onkey)

        canvas = FigureCanvasTkAgg(fig, master=frame_plot)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

        print("\n📌 Instrucciones:")
        print("✔ Click izquierdo = línea 1")
        print("✔ Click derecho = línea 2")
        print("✔ Click del medio = limpiar")
        print("✔ Presionar 'C' para calcular liquidez no saldada entre ambas líneas\n")

    except Exception as e:
        print(f"⚠️ Error: {e}")

# === Botón recargar
tk.Button(frame_inputs, text="Recargar", command=recargar, bg="gray20", fg="white").grid(row=0, column=6, padx=10)

root.mainloop()
