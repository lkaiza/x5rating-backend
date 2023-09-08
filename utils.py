def is_nan(x):
    return x != x


def shop_data_to_map(row):
    return {
        "id": row[0],
        "title": row[1],
        "coordinates": row[2],
        "address": row[3],
        "rating": row[4]["ratingValue"],
        "aspects": row[10]
    }
