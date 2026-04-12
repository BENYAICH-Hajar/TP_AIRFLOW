#!/usr/bin/env python3
"""
Générateur de logs Apache Combined Log Format pour simulation e-commerce.
Usage : python3 generer_logs.py <date> <nb_lignes> <fichier_sortie>
Exemple : python3 generer_logs.py 2024-03-15 1000 /tmp/access_2024-03-15.log
"""

import random
import sys
from datetime import datetime, timedelta

IPS = [
    "92.184.12.44", "185.220.101.12", "78.23.145.67", "213.95.11.88",
    "5.188.10.132", "91.108.4.15", "176.31.208.51", "62.210.114.199",
    "37.187.0.200", "82.66.14.25", "90.54.12.144", "51.15.201.77",
    "109.190.122.6", "212.47.234.67", "163.172.0.100",
]

URLS = [
    ("GET", "/produit/smartphone-samsung-galaxy-s24-ultra", 200, 48200),
    ("GET", "/produit/laptop-dell-inspiron-15-amd", 200, 72100),
    ("GET", "/produit/casque-audio-sony-wh1000xm5", 200, 38900),
    ("GET", "/produit/aspirateur-dyson-v15", 200, 41000),
    ("GET", "/produit/montre-connectee-apple-watch-9", 200, 55300),
    ("GET", "/categorie/informatique", 200, 22400),
    ("GET", "/categorie/electromenager", 200, 19800),
    ("GET", "/categorie/smartphones", 200, 25100),
    ("GET", "/panier", 200, 8900),
    ("POST", "/checkout/valider", 200, 3400),
    ("POST", "/checkout/paiement", 200, 4100),
    ("GET", "/produit/article-retire-de-vente", 404, 512),
    ("GET", "/admin/dashboard", 403, 287),
    ("GET", "/produit/inexistant-xyz", 404, 512),
    ("POST", "/api/panier/ajouter", 500, 1024),
    ("GET", "/api/stock/verifier", 503, 890),
    ("GET", "/static/css/main.min.css", 200, 18200),
    ("GET", "/static/js/bundle.min.js", 200, 234100),
    ("GET", "/static/img/logo.png", 200, 4200),
    ("GET", "/favicon.ico", 200, 1150),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/121.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "python-requests/2.31.0",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]

REFERRERS = [
    "https://www.google.fr/search?q=smartphone+pas+cher",
    "https://www.google.fr/shopping",
    "https://www.bing.com/search?q=laptop+dell",
    "https://www.lesnumeriques.com/",
    "-",
    "-",
    "-",
]

def generer_log_line(date_str: str) -> str:
    ip = random.choice(IPS)
    methode, url, status, taille = random.choice(URLS)
    user_agent = random.choice(USER_AGENTS)
    referrer = random.choice(REFERRERS)

    base = datetime.strptime(date_str, "%Y-%m-%d")
    delta = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    timestamp = (base + delta).strftime("%d/%b/%Y:%H:%M:%S +0100")

    return (
        f'{ip} - - [{timestamp}] "{methode} {url} HTTP/1.1" '
        f'{status} {taille} "{referrer}" "{user_agent}"'
    )

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 generer_logs.py <date YYYY-MM-DD> <nb_lignes> <fichier_sortie>")
        sys.exit(1)

    date_str = sys.argv[1]
    nb_lignes = int(sys.argv[2])
    fichier_sortie = sys.argv[3]

    with open(fichier_sortie, "w", encoding="utf-8") as f:
        for _ in range(nb_lignes):
            f.write(generer_log_line(date_str) + "\n")

    print(f"[OK] {nb_lignes} lignes générées dans {fichier_sortie}")

if __name__ == "__main__":
    main()