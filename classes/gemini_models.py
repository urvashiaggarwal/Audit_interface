import os
import json
import mysql.connector
import pandas as pd
from pydantic import BaseModel
from typing import Union, List
#from google import genai
#from google.genai import types
import google.generativeai as genai
#from google.genai import types
from google.generativeai import types
from dotenv import load_dotenv

load_dotenv()

# Pydantic model 
class DataPointScore(BaseModel):
    data_point_name: str
    index: int
    score: int

bible_propt = r"C:\Users\abhishek.a3\Desktop\python\audit\P-S MS\prompt\bible_prompts.json"
#Gemini Client
class GeminiClient:
    def __init__(self, model_name='gemini-2.5-flash-preview-04-17', prompt_path=bible_propt):
        api_key = os.getenv("GEMINI_API_KEY")
        
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")
        #self.__client = genai.Client(api_key=api_key)
        self.__client = genai.configure(api_key=api_key)
        self.__model_name = model_name
        self.__prompt_path = prompt_path

    def __load_prompt(self, prompt_key: str) -> str:
        with open(self.__prompt_path, 'r') as file:
            data = json.load(file)
        return data.get(prompt_key, "")

    def get_scores(self, input_df: pd.DataFrame, data_point: str, instruction: str) -> List[DataPointScore]:
        prompt = self.__load_prompt("data_point_evaluator").format(instruction=instruction)

        '''*contents = [
            types.Content(role="user", parts=[
                types.Part.from_text(text=input_df.to_json(orient='records'))
            ])
        ]'''
        model = genai.GenerativeModel(self.__model_name)
        contents = model.generate_content(input_df.to_json(orient='records'))
        '''config = types.GenerateContentConfig(
            temperature=0.8,
            response_mime_type="application/json",
            response_schema=list[DataPointScore],
            system_instruction=[types.Part.from_text(text=prompt)],
        )'''
        config = {
            "temperature": 0.8,
            "top_p": 1,
            "top_k": 1
        }

        response = self.__client.models.generate_content(
            model=self.__model_name,
            contents=contents,
            config=config
        )

        raw_text = response.candidates[0].content.parts[0].text.strip()
        print(f"Raw response for {data_point}: {raw_text}")

        if not raw_text:
            raise ValueError(f"Empty response for {data_point}")

        try:
            parsed = json.loads(raw_text)
            return [DataPointScore(**item) for item in parsed]
        except Exception as e:
            print(f"Validation failed: {e}")
            raise
