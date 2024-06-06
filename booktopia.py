# In this script to scrape the book details we hit one request
# then using soup we parse the html content & fetch details from desired script
# To run the script dynamically for input_list of isbn directly from googledriveurl, pip install gdown
# This script also handles max_workers for threadpool according to OS CPU cores


import json
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import gdown
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from fake_useragent import UserAgent

# Create a UserAgent object
user_agent = UserAgent()

# Define headers for the HTTP requests
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": user_agent.random
}

# Function to fetch book details using ISBN
def book_extraction(isbn):
    url = f"https://www.booktopia.com.au/book/{isbn}.html"  # URL to fetch the book details
    print(url)  # Print the URL for debugging
    response = requests.get(url, headers=headers)  # Send a GET request to the URL
    if response.status_code == 200:  # Check if the request was successful
        soup = BeautifulSoup(response.content, 'html.parser')  # Parse the response content

        # Find the #__NEXT_DATA__ script tag containing the book data
        script_tag = soup.find("script", id="__NEXT_DATA__")
        if script_tag:
            # Extract the JSON data from the script tag
            json_data = json.loads(script_tag.string)

            # Extract book details from the JSON data
            book_details = json_data['props']['pageProps']['product']
            title = book_details['displayName']  # Get the book title

            # Extract author(s) from the contributors list
            contributors = book_details['contributors']
            author_names = [contributor['name'] for contributor in contributors]

            # Extract retail price and sale price
            retail_price = book_details.get('retailPrice')
            sale_price = book_details.get('salePrice')

            # Extract book type
            book_type = book_details.get('bindingFormat')

            # Extract published date, publisher, and number of pages
            published_date = book_details.get('publicationDate')
            if published_date:
                # Parse and format the published date if it's provided
                published_date = datetime.strptime(published_date, '%Y-%m-%d').strftime('%Y-%m-%d')
            else:
                published_date = "Publication date not available"

            publisher = book_details.get('publisher')
            num_pages = book_details.get('numberOfPages')

            # Extract ISBN-10
            isbn_10 = book_details.get('isbn10')

            # Return a dictionary containing the book details
            return {
                "Title of the Book": title,
                "Author/s": author_names,
                "Book type": book_type,
                "Original Price (RRP)": retail_price,
                "Discounted price": sale_price,
                "ISBN-10": isbn_10,
                "Published Date": published_date,
                "Publisher": publisher,
                "No. of Pages": num_pages
            }
        else:
            print(f"No #__NEXT_DATA__ script tag found for ISBN: {isbn}")
            return {"Title of the Book": f"Book not found for ISBN {isbn}"}
    else:
        print(f"Book not found for ISBN: {isbn}")
        return {"Title of the Book": f"Book not found for ISBN {isbn}"}

# Main function to read the input CSV, fetch book details, and write to an output CSV
def main():
    google_drive_url = "https://drive.google.com/uc?id=1u4f-SSnZsgleZCK0533EC5VJauoFHjuM"  # Modified Google Drive URL for direct download
    input_file_path = "input_list.csv"  # Local path to save the downloaded file
    output_file_path = "book_details.csv"  # Output CSV file path

    # Download the CSV file from Google Drive
    gdown.download(google_drive_url, input_file_path, quiet=False)

    # Open the input CSV file
    with open(input_file_path, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)  # Read the input CSV file

        # Collect all ISBNs
        isbns = [row["ISBN13"] for row in reader]

    # Determine the number of workers based on CPU cores, default to 4 if more than 4 available
    num_cores = os.cpu_count()
    num_workers = min(num_cores, 4) if num_cores is not None else 4

    # List to store the results
    results = []

    # Use ThreadPoolExecutor to fetch book details concurrently
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit tasks to the executor
        future_to_isbn = {executor.submit(book_extraction, isbn): isbn for isbn in isbns}

        # Process the completed tasks
        for future in as_completed(future_to_isbn):
            isbn = future_to_isbn[future]
            try:
                book_details = future.result()
                results.append(book_details)  # Collect the book details
            except Exception as exc:
                print(f"Error fetching details for ISBN {isbn}: {exc}")
                results.append({"Title of the Book": f"Error fetching details for ISBN {isbn}"})

    # Write all collected results to the output CSV file
    with open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:
        fieldnames = ["Title of the Book", "Author/s", "Book type", "Original Price (RRP)",
                      "Discounted price", "ISBN-10", "Published Date", "Publisher", "No. of Pages"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)  # Initialize the CSV writer
        writer.writeheader()  # Write the header to the output CSV file

        # Write the results to the output CSV file
        for result in results:
            writer.writerow(result)

    print(f"Book details saved to {output_file_path}")  # Print a confirmation message

# Run the main function if the script is executed directly
if __name__ == "__main__":
    main()