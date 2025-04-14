from setuptools import setup, find_packages

setup(
    name="chatbot",
    version="0.1.0",
    author="Pranav Shrihari",
    description="An interactive chatbot for working with SQLite using OpenAI.",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "openai"
    ],
    entry_points={
        'console_scripts': [
            'chatbot=chatbot.main:chatbot_interaction',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)
