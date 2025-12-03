import pandas as pd
import io
from source.core.dto import ParseResult


def _force_text(value) -> str:
    if value is None or value == "":
        return ""
    str_val = str(value).strip()
    if "." in str_val or "," in str_val:
        if str_val.replace(".", "").replace(",", "").replace("-", "").isdigit():
            return "'" + str_val
        if any(c in str_val for c in ".") and str_val.replace(".", "").isdigit():
            return "'" + str_val
    return str_val


def result_to_csv_bytes(result: ParseResult) -> bytes:
    rows = []
    for p in result.products:
        in_stock = getattr(p, "in_stock", True)  
        in_stock_text = "Есть" if in_stock else "Нет"

        rows.append({
            "product_id": p.product_id,
            "name": (getattr(p, "name", "") or "").strip(),
            "price": _force_text(getattr(p, "price", "")),
            "old_price": _force_text(getattr(p, "old_price", "")) if getattr(p, "old_price", None) else "",
            "calories": _force_text(getattr(p, "calories", "")) if getattr(p, "calories", None) else "",
            "proteins": _force_text(getattr(p, "proteins", "")) if getattr(p, "proteins", None) else "",
            "fats": _force_text(getattr(p, "fats", "")) if getattr(p, "fats", None) else "",
            "carbs": _force_text(getattr(p, "carbs", "")) if getattr(p, "carbs", None) else "",
            "weight": getattr(p, "weight", "") or "",
            "ingredients": str(getattr(p, "ingredients", "") or "").replace("\n", " ").replace("\r", ""),
            "photos": " | ".join(getattr(p, "photos", [])[:5]) if getattr(p, "photos", None) else "",
            "category": getattr(p, "category", "") or "",
            "store": getattr(p, "store", "") or "",
            "Наличие": in_stock_text,
        })

    df = pd.DataFrame(rows)

    column_order = [
        "product_id", "name", "store", "category",
        "price", "old_price", "Наличие",  
        "calories", "proteins", "fats", "carbs",
        "weight", "ingredients", "photos"
    ]
    df = df.reindex(columns=column_order)

    output = io.StringIO()
    df.to_csv(
        output,
        sep=";",
        index=False,
        encoding="utf-8-sig",
        lineterminator="\n"
    )
    return output.getvalue().encode("utf-8-sig")