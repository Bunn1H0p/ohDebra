import pdfplumber

path = "input/Dexter_1x01_-_Pilot.pdf"
with pdfplumber.open(path) as pdf:
    first = pdf.pages[0].extract_text() or ""
print(first[:1000])
