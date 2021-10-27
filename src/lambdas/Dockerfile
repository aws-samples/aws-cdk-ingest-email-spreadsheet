FROM public.ecr.aws/lambda/python:3.8
COPY email_filtering.py  ./email_filtering.py
COPY email_processing.py  ./email_processing.py
COPY requirements.txt ./email_processing.txt
COPY requirements.txt ./
RUN pip install -r requirements.txt
CMD ["email_filtering.lambda_handler"]