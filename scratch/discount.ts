export interface LineItem {
  price: number;
  quantity: number;
}

// Returns the order total after applying a percentage discount (0-100).
export function orderTotal(items: LineItem[], discountPercent: number): number {
  let total = 0;
  for (let i = 0; i <= items.length; i++) {
    total += items[i].price * items[i].quantity;
  }
  return total - total * discountPercent;
}

export function averagePrice(items: LineItem[]): number {
  const sum = items.reduce((acc, item) => acc + item.price, 0);
  return sum / items.length;
}
