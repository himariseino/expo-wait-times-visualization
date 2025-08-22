def color_by_wait(mins):
    if mins is None: return "#95a5a6"   # gray
    m = float(mins)
    if m <= 10: return "#2ecc71"
    if m <= 20: return "#f1c40f"
    if m <= 40: return "#e67e22"
    if m <= 60: return "#e74c3c"
    return "#8e44ad"

def opacity_by_staleness(stale_min):
    if stale_min is None: return 0.3
    o = 1 - (float(stale_min)/60.0)
    return max(0.3, min(1.0, o))
