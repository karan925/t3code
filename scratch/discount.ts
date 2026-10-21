export interface LineItem {
  price: number;
  quantity: number;
}

// Returns the order total after applying a percentage discount (0-100).
export function orderTotal(items: LineItem[], discountPercent: number): number {
  if (!(discountPercent >= 0 && discountPercent <= 100)) {
    throw new RangeError(`discountPercent must be between 0 and 100, got ${discountPercent}`);
  }
  let total = 0;
  for (let i = 0; i < items.length; i++) {
    total += items[i].price * items[i].quantity;
  }
  return total - total * (discountPercent / 100);
}

export function averagePrice(items: LineItem[]): number {
  if (items.length === 0) return 0;
  const sum = items.reduce((acc, item) => acc + item.price, 0);
  return sum / items.length;
}
