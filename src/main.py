# Requests Tutorial ..
import requests

req = requests
response = req.get("https://reqres.in/api/products/3")

# Mostly Interested Metadata Information from the response object - Status Code, Encoding, URL, Headers
print(response.status_code)

# URL will be useful to validate while redirecting or checking the query string
print(response.url)
print(response.cookies)
print(response.elapsed)

# Encoding can also be modified before reading the reponse content, for example
# response.encoding = 'ISO-8859-1'
# print(response.encoding)
print(response.encoding)
print(response.text)

# Text argument prints the content of the reponse as plain string
print(response.text)
print(response.history)

# From Headers Content-Type, Date, Content Type, Status will be useful to take further steps. Most of the time it works case insensitive like below
print(response.headers)
print(response.headers['coNtent-tYpE'])
print(response.headers['dATe'])

# Json method converts the content of the response to Json Object (dict - kV pairs) 
json_data = response.json()
print(type(json_data))
print(json_data)

print(json_data['data']['id'])
print(json_data["support"]["text"])

# Loop through the key value pairs
for k, v in json_data.items():
    print (k , ":" , v)

response.close()



