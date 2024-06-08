import requests
import pandas
import psycopg2

url = "https://yahoo-finance127.p.rapidapi.com/key-statistics/aapl"

headers = {
	"x-rapidapi-key": "26e97ffc1cmsh57bb80fee2686cep1aebdbjsn7380f6dc9061",
	"x-rapidapi-host": "yahoo-finance127.p.rapidapi.com"
}

response = requests.get(url, headers=headers)
responseData = response.json()

