import pandas as pd
import re

class DataPreprocessing:
    def __init__(self, filename=None):
        # filename is optional now since we're accepting DataFrames directly
        self.filename = filename

    def labellingData(self, n='full', save=True):
        data_hooligans = pd.read_csv(self.filename)
        data_hooligans_fulltext = data_hooligans['full_text']

        def label_data(text_series):
            labels = []

            for i, text in enumerate(text_series):
                print(f'Text {i+1}/{len(text_series)}:')
                print(text)
                label = input('Enter Label: ')
                labels.append(label)
                print('\n')

            labeled_data = pd.DataFrame({'text': text_series, 'label': labels})
                
            return labeled_data
        
        if type(n) == str:
            labeled_data = label_data(data_hooligans_fulltext)
        else:
            labeled_data = label_data(data_hooligans_fulltext[:n])

        return labeled_data
    
    def clean_text(self, data, col_name='full_text', date_col='created_at'):
        # Use the DataFrame directly instead of reading from a file
        if isinstance(data, pd.DataFrame):
            data_fulltext = data[col_name]
        else:
            raise ValueError("Input data must be a DataFrame.")
        
        # Define the cleaning function
        def cleanUp(text):
            replacements = {
                r'\blg\b': 'lagi',
                r'\bbgt\b': 'sangat',
                r'\bsm\b': 'sama',
                r'\bntu\b': 'itu',
                r'\+\b': '',                # Remove plus sign
                r'\bkyk\b': 'kayak',
                r'\bak\b': 'aku',
                r'\bato\b': 'atau',
                r'\bd\b': 'di',
                r'\bIndo\b': 'indonesia',
                r'\bjd\b': 'jadi',
                r'\bskrg\b': 'sekarang',
                r'\b\\b': 'atau',
                r'\bgak\b': 'tidak',
                r'\bga\b': 'tidak'
            }

            # Apply each replacement pattern to the text
            for pattern, replacement in replacements.items():
                text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
                
            # Remove mentions like @username
            text = re.sub(r'@\w+', '', text)
            
            # Remove hashtags like #hashtag
            text = re.sub(r'#\w+', '', text)
            
            # Replace multiple dots with a single dot
            text = re.sub(r'\.{2,}', '.', text)
            text = re.sub(r'\.{2,}', '. ', text)
            
            # Fix patterns like 'ngalah2in' -> 'ngalah ngalahin'
            text = re.sub(r'(\w+)(\d)(\w+)', lambda m: f"{m.group(1)} {m.group(1)}{m.group(3)}", text)
            
            # Split words with numbers in the middle (e.g., "macam2" -> "macam macam")
            text = re.sub(r'(\w+)(\d)(\w*)', lambda m: f"{m.group(1)} {m.group(1)}{m.group(3)}", text)
            
            # Separate numbers and words (e.g., "20biji" -> "20 biji")
            text = re.sub(r'(\d+)([a-zA-Z]+)', r'\1 \2', text)
            
            # Remove links
            text = re.sub(r'https://t\.co/\S+', '', text)
            
            # Remove extra spaces from the start and end
            return text.strip()
        
        # Apply text cleaning to the full text column
        data[col_name] = data[col_name].apply(cleanUp)

        # Drop unnecessary columns
        columns_to_drop = [
            'conversation_id_str', 'favorite_count', 'id_str', 'image_url',
            'in_reply_to_screen_name', 'lang', 'location', 'quote_count',
            'reply_count', 'retweet_count', 'tweet_url', 'user_id_str', 'username'
        ]
        data = data.drop(columns=columns_to_drop, errors='ignore')

        # Format the date column if it exists and convert to only date (no time)
        if date_col in data.columns:
            data[date_col] = pd.to_datetime(data[date_col], format='%a %b %d %H:%M:%S +0000 %Y', errors='coerce')
            # Only keep the date (without time)
            data[date_col] = data[date_col].dt.date

        # Sort data by the date column (from newest to oldest)
        data = data.sort_values(by=date_col, ascending=False).reset_index(drop=True)

        return data
