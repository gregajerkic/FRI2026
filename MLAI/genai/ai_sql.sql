SELECT AI_COMPLETE(
    'llama3.1-8b',
    'Explain the difference between a data lake and a data warehouse in two sentences.'
) AS llm_response;
  
-- =========================================================================
-- 2. AI_SENTIMENT  — Sentiment scoring
-- =========================================================================
SELECT
    review,
    AI_SENTIMENT(review) AS sentiment_score
FROM (VALUES
    ('Absolutely love this product! Best purchase I have ever made.'),
    ('It is okay, nothing special. Works as expected.'),
    ('Terrible experience. The item broke on the first day.'),
    ('Decent quality for the price, but shipping was slow.'),
    ('Outstanding customer support — they resolved my issue in minutes!')
) AS t(review);

-- =========================================================================
-- 3. AI_CLASSIFY  — Classify text into user-defined categories
-- =========================================================================
SELECT
    message,
    AI_CLASSIFY(
        message,
        ['Billing', 'Technical Support', 'Account Management', 'General Inquiry']
    ) AS category
FROM (VALUES
    ('I was charged twice on my last invoice.'),
    ('My dashboard keeps showing a 500 error.'),
    ('How do I add another user to my team plan?'),
    ('What new features are coming this quarter?'),
    ('I need a refund for my last payment.')
) AS t(message);

-- =========================================================================
-- 4. AI_FILTER  — Boolean filter using natural language conditions
-- =========================================================================
SELECT
    product_name,
    description,
    AI_FILTER(
        PROMPT('Is this product suitable for outdoor use? Product: {0}', description)
    ) AS is_outdoor
FROM (VALUES
    ('Trail Runner 3000',   'Waterproof hiking shoes with rugged Vibram soles for all-terrain use.'),
    ('Silk Lounge Set',     'Ultra-soft indoor loungewear made of 100% mulberry silk.'),
    ('Solar Power Bank',    'Portable 20000mAh battery with solar charging panel for camping trips.'),
    ('Espresso Machine Pro','Countertop espresso machine with a built-in grinder and milk frother.')
) AS t(product_name, description);

-- =========================================================================
-- 5. AI_REDACT  — Redact sensitive information from text
-- =========================================================================
SELECT AI_REDACT(
  input => 'My name is John and I live at twenty third street, San Francisco. My phone number is 123-456-7890.',
  categories => ['NAME', 'ADDRESS']
  );

-- =========================================================================
-- 6. AI_EXTRACT  — Extract structured info from unstructured text
-- =========================================================================
SELECT AI_EXTRACT(
  text => 'John Smith lives in San Francisco and works for Snowflake',
  responseFormat => {'name': 'What is the first name of the employee?', 'city': 'What is the address of the employee?'}
);

SELECT AI_EXTRACT(
  file => TO_FILE('@LSP_DB.CORTEX_DEMO.file_stage', 'A review of simheuristics.pdf'),
  responseFormat => {
    'schema': {
      'type': 'object',
      'properties': {
        'title': {
          'description': 'What is the title of document?',
          'type': 'string'
        },
        'author': {
          'description': 'Who is the author of the document?',
          'type': 'string'
        }
      }
    }
  }
);