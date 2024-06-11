# lazy_fred
![example workflow](https://github.com/Jmetrics86/lazy_fred//actions/workflows/python-app.yml/badge.svg)

# lazy_fred: Effortless FRED Data Collection

lazy_fred is a Python library designed to simplify the process of collecting economic data from the Federal Reserve Economic Data (FRED) API. It automates searching for relevant series, filtering by frequency and popularity, and exporting the data into convenient CSV files.

## Features

- **Automated Search:** Searches FRED across various economic categories.
- **Filtered Selection:** Selects series based on popularity and frequency (daily, weekly, monthly).
- **Error Handling:** Includes retry mechanisms and logging for robust data collection.
- **CSV Export:** Saves the collected data in separate CSV files for easy analysis.

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-username/lazy_fred.git
   ```

2. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Obtain a FRED API Key:**

   - Visit the [FRED website](https://fred.stlouisfed.org/docs/api/fred/) and get your free API key.
   - Create a `.env` file in the project root directory and add your API key:

     ```
     API_KEY=your_api_key_here
     ```

## Usage

1. **Configure Search Categories:**
   - Modify the `search_categories` list in the script to include the categories of interest.

2. **Run the Script:**

   ```bash
   python lazy_fred.py
   ```

3. **Output:**
   - The script will create three CSV files in your project directory:
     - `lazy_fred_Search.csv`: Contains the search results from FRED.
     - `daily_data.csv`: Contains daily time series data.
     - `monthly_data.csv`: Contains monthly time series data.
     - `weekly_data.csv`: Contains weekly time series data.

## Code Structure

- **`AccessFred` Class:**
  - Handles API key retrieval and validation.

- **`collect_categories` Class:**
  - Executes searches across categories and consolidates results.

- **`daily_export`, `monthly_export`, and `weekly_export` Classes:**
  - Filter series by frequency and popularity.
  - Collect and export time series data.

## Contributions

Contributions are welcome! Feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License.

## Disclaimer

This library is not affiliated with or endorsed by the Federal Reserve Bank of St. Louis or the FRED project.

## Acknowledgments

- This project utilizes the `fredapi` library for interacting with the FRED API.

## Contact

For any questions or feedback, please open an issue in the repository.
