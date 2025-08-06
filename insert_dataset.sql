-- Make sure to use a valid UUID or generate one dynamically
INSERT INTO dataset (id, name, description, created_by, created_at)
VALUES (
    'hausa',  -- Replace with your desired or with the language name instead
    'Sample Dataset',
    'This is a test dataset entry',
    '16c449a6-9d35-45d9-996d-45febd413282',  -- Replace with your user ID
    NOW()
);
