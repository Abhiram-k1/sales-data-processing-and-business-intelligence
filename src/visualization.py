"""Plot only computed Spark aggregate results."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def save_charts(tables, figures):
    charts = [
        ("category_revenue", "category", "total_revenue", "Category revenue", "Revenue (INR, configured conversion)"),
        ("location_revenue", "store_location", "total_revenue", "Market revenue", "Revenue (INR, configured conversion)"),
        ("top_five_products", "product_name", "quantity_sold", "Top five products by units sold", "Units sold"),
    ]
    for name, label, value, title, ylabel in charts:
        rows = tables[name].limit(20).collect()
        figure, axis = plt.subplots(figsize=(11, 6))
        axis.barh([row[label] for row in rows][::-1], [float(row[value]) for row in rows][::-1])
        axis.set_title(title + (" (largest 20)" if name != "top_five_products" else ""))
        axis.set_xlabel(ylabel)
        figure.tight_layout()
        figure.savefig(figures / f"{name}.png", dpi=150)
        plt.close(figure)
