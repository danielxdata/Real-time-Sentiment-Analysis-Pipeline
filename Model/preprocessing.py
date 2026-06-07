import re
from nltk.tokenize import word_tokenize ,sent_tokenize
from nltk.corpus import stopwords
import string


def clean_text(text):
    
    text = re.sub(r'<.*?>', '', text)
    text = text.replace("\\", "")
    return text



def tokenise_text(text):
    
    sentences=sent_tokenize(text)
    words=[]
    for sentence in sentences:
        word=word_tokenize(sentence)
        words.append(word)
    return words



def remove_stop_words(text):

    stop_words=set(stopwords.words("english"))
    punct_marks=string.punctuation
    cleaned_sentences=[]
    for sentences in text:
        cleaned_words=[]
        for word in sentences:
            if word not in stop_words and word not in punct_marks:
                cleaned_words.append(word)
        cleaned_sentences.append(cleaned_words)
    return cleaned_sentences




def join_tokens(review):

    full_text = []
    for sentence in review:
        joined_sentence = " ".join(sentence)
        full_text.append(joined_sentence)
    return " ".join(full_text)

def preprocess_step (revew) :
   step1 =  clean_text(revew)
   step2 = tokenise_text(step1)
   step3= remove_stop_words(step2)
   return  join_tokens(step3)