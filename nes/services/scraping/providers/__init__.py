"""LLM Provider implementations for the Scraping Service.

This module provides various LLM provider implementations for data normalization,
translation, and relationship extraction tasks.

Supported Providers:
    - Mock: Testing and development provider (no API calls)
    - AWS Bedrock: Amazon's managed LLM service
    - OpenAI: OpenAI's GPT models (future)
    - Google: Google's Gemini models (future)
    - Anthropic: Anthropic's Claude models (future)

Provider Interface:
    All providers implement the BaseLLMProvider interface for:
    - Text generation
    - Structured data extraction
    - Translation
    - Token usage tracking (optional)

Usage:
    >>> # Use mock provider for testing
    >>> from nes.services.scraping.providers import MockLLMProvider
    >>> provider = MockLLMProvider()
    >>> response = await provider.generate_text(prompt="Translate: राम चन्द्र पौडेल")
    
    >>> # Use AWS Bedrock for production
    >>> from nes.services.scraping.providers import AWSBedrockProvider
    >>> provider = AWSBedrockProvider(
    ...     region_name="us-east-1",
    ...     model_id="anthropic.claude-3-sonnet-20240229-v1:0"
    ... )
    >>> response = await provider.generate_text(prompt="Translate: राम चन्द्र पौडेल")
"""

from .aws import AWSBedrockProvider
from .base import BaseLLMProvider
from .mock import MockLLMProvider
from .openai import OpenAIProvider

__all__ = ["BaseLLMProvider", "MockLLMProvider", "AWSBedrockProvider", "OpenAIProvider"]
