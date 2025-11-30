import pandas as pd


class DataAnalyser:

    def __init__(self, data):
        self.data = data
        self.df: pd.DataFrame = pd.DataFrame(data)

    def total_revenue(self) -> float:
        return (self.df.price * self.df.quantity).sum()

    def items_by_category(self) -> dict:
        res = dict()
        for value in self.data:
            item = value.get("item")
            category = value.get("category")
            if res.get(category):
                res[category].append(item)
            else:
                res[category] = [item]
        return res

    def expensive_purchases(self, min_price) -> list:
        return self.df.query(f"price >= {min_price}").to_dict(orient='records')

    def average_price_by_category(self) -> dict:
        return self.df[["category", "price"]].groupby("category").mean("price").sort_values("price").to_dict().get('price')

    def most_frequent_category(self) -> str:
        return self.df.groupby("category").count().sort_values("item", ascending=False).reset_index()["category"][0]




purchases = [
    {"item": "apple", "category": "fruit", "price": 1.2, "quantity": 10},
    {"item": "banana", "category": "fruit", "price": 0.5, "quantity": 5},
    {"item": "milk", "category": "dairy", "price": 1.5, "quantity": 2},
    {"item": "bread", "category": "bakery", "price": 2.0, "quantity": 3},
]



purchase_analyser = DataAnalyser(purchases)

if __name__ == "__main__":
    print("Общая выручка:", purchase_analyser.total_revenue())
    print("Товары по категориям:", purchase_analyser.items_by_category())
    print("Покупки дороже 1.0:", purchase_analyser.expensive_purchases(1.0))
    print("Средняя цена по категориям:", purchase_analyser.average_price_by_category())
    print("Категория с наибольшим количеством проданных товаров:", purchase_analyser.most_frequent_category())