from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GreetingOperator(BaseOperator):

    @apply_defaults
    def __init__(self, greeting, name, **kwargs):
        super(GreetingOperator, self).__init__(**kwargs)
        self.greeting = greeting
        self.name = name

    def execute(self, context):
        self.log.info(f"{context}")

        greeting_message = f"{self.greeting}, {self.name}"
        self.log.info(greeting_message)

        return greeting_message