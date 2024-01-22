# Project Description

## Overview

This Python script demonstrates a simple implementation of user profile caching using Redis. It fetches user profiles from a mock database (static JSON file) and stores them in a Redis cache for faster retrieval.

## Workflow

1. **Set up Redis Connection:**
   - The script establishes a connection to a Redis server running on `localhost:6379`.

2. **Mock Database:**
   - A static JSON file named `user_profiles1.json` serves as the mock database, containing user profiles.

3. **Fetch User Profile Function:**
   - The `fetch_user_profile` function reads the mock database file and retrieves a user's profile based on their ID.

4. **Caching with Redis:**
   - The `get_user_profile` function checks if the user's profile is already in the Redis cache.
   - If found, it retrieves the profile from the cache.
   - If not found, it fetches the profile from the mock database, stores it in the Redis cache, and then retrieves and returns the profile.

5. **Example Usage:**
   - An example usage is provided at the end of the script:
     ```python
     user_id_to_fetch = '123'
     user_profile = get_user_profile(user_id_to_fetch)
     print(f"Fetched user profile: {user_profile}")
     ```
     This demonstrates how to use the caching mechanism to fetch a user's profile.

## Usage

1. **Install Dependencies:**
   ```bash
   pip install redis

