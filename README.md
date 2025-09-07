# SproutBox

SproutBox is a lightweight tool designed to work with APIs and file storage. It requires API keys and folder IDs to function, which are stored in a `.env` file.

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/sproutbox.git
   cd sproutbox
Use the .env for the basic logic of what you need to add to make the program run effectively. You will need the folder ID from Box and SproutVideo, which can be found at the end of the URL of the folder you click on. 

On Box's end, you will need to create and register an app to gain access to the dev token. This can be found in the dev console. You, then, can generate a token that lasts 60 minutes to transfer whatever you want. Make sure to add the token to the line in the env before beginning. 

Install dependencies
Run:
```
pip install -r requirements.txt
```
```
python sproutbox.py
```

Note: make sure you are not on any VPN with SSL inspection like ZScaler, the program will break. 
