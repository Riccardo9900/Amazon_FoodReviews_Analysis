#!/usr/bin/env python3
import sys
from collections import defaultdict, Counter

product_reviews = defaultdict(lambda: defaultdict(list))
    
for line in sys.stdin:
    line = line.strip()
    
    try:
        year, product_id, review = line.split('\t')    
        product_reviews[year][product_id].append(review)

    except ValueError:
        continue
    
for year, products in product_reviews.items():
    print(f'Anno: {year}')
    
    top_products = Counter(products).most_common(10)
   
    for product_id, count in top_products:
        
        reviews = products[product_id]
        
        word_counts = Counter()
        
        for review in reviews:
            words = review.split()
            
            word_counts.update(words)
        
        top_words = [(word, word_count) for word, word_count in word_counts.items() if len(word) >= 4]
        top_words = sorted(top_words, key=lambda x: x[1], reverse = True)[:5]
        print(f'Prodotto: {product_id} - Numero di recensioni: {len(reviews)}')
        
        
        for word, word_count in top_words:
            print(f'Parola: {word} - Numero di occorrenze: {word_count}')
        
        print()
