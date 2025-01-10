-- Add poi_set column
ALTER TABLE poi_notifications 
ADD COLUMN IF NOT EXISTS poi_set BYTEA[];

-- Update existing rows with empty array
UPDATE poi_notifications 
SET poi_set = ARRAY[]::bytea[] 
WHERE poi_set IS NULL;

-- Make column not null after filling defaults
ALTER TABLE poi_notifications 
ALTER COLUMN poi_set SET NOT NULL; 