# Type Mappings Feature

## Overview
The Type Mappings feature allows users to define custom Python functions that transform column types when comparing data between different systems (e.g., Netezza ↔ Databricks).

## Key Design Decisions

### 1. Non-Directional System Pairs
- Transformations are stored per system pair, not per direction
- `(SystemA, SystemB)` = `(SystemB, SystemA)`
- Database constraint ensures uniqueness using `LEAST/GREATEST`

### 2. Standardized Function Signature
All transformation functions must follow this signature:
```python
def transform_columns(column_name: str, data_type: str) -> str:
    """
    Returns SQL expression string for column transformation.
    
    Args:
        column_name: Name of the column
        data_type: Data type of the column
    
    Returns:
        SQL expression string (e.g., "CAST(col AS VARCHAR)" or "col")
    """
```

### 3. Execution Strategy (Option 1 - Simple)
In `run_validation.py`, functions are executed using:
```python
# Load function code from database
system_a_code = get_transformation_code(system_a_id, system_b_id)

# Execute dynamically
exec(system_a_code)  # Creates transform_columns() in namespace
transformed_columns = [transform_columns(col_name, col_type) for col, col_type in columns]
```

## Database Schema

### New Table: `control.type_transformations`
```sql
CREATE TABLE control.type_transformations (
  id                BIGSERIAL PRIMARY KEY,
  system_a_id       BIGINT NOT NULL REFERENCES control.systems(id) ON DELETE CASCADE,
  system_b_id       BIGINT NOT NULL REFERENCES control.systems(id) ON DELETE CASCADE,
  system_a_function TEXT NOT NULL,  -- Python function for system A
  system_b_function TEXT NOT NULL,  -- Python function for system B
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by        TEXT NOT NULL,
  version           INTEGER NOT NULL DEFAULT 1,
  
  -- Ensure non-directional uniqueness
  CONSTRAINT unique_system_pair UNIQUE (
    LEAST(system_a_id, system_b_id), 
    GREATEST(system_a_id, system_b_id)
  ),
  -- Prevent self-referential pairs
  CONSTRAINT different_systems CHECK (system_a_id != system_b_id)
);
```

## Backend Implementation

### API Endpoints
- `GET /api/type-transformations` - List all transformations
- `GET /api/type-transformations/{systemAId}/{systemBId}` - Get specific pair
- `POST /api/type-transformations` - Create new transformation
- `PUT /api/type-transformations/{systemAId}/{systemBId}` - Update transformation
- `DELETE /api/type-transformations/{systemAId}/{systemBId}` - Delete transformation
- `GET /api/type-transformations/default/{systemKind}` - Get default function for system type
- `POST /api/validate-python` - Validate Python code (syntax + structure + mypy)

### Default Transformations
Pre-built transformation functions for common system types:
- **Databricks/Spark**: Handles DoubleType, DecimalType, StringType
- **Netezza**: Handles DOUBLE PRECISION, CHAR types, NUMERIC
- **Postgres**: Handles numeric types and character trimming
- **MySQL**: Similar to Postgres
- **SQLServer**: Similar to Postgres
- **Generic**: Casts everything to VARCHAR(250)

### Code Validation
Multi-level validation before saving:
1. **Syntax Check**: Uses `ast.parse()` to validate Python syntax
2. **Structure Check**: Ensures function is named `transform_columns`
3. **Signature Check**: Validates exactly 2 parameters
4. **Type Hints Check**: Optional mypy validation (if installed)

## Frontend Implementation

### New View: Type Mappings
Location: `frontend/src/views/TypeMappingsView.jsx`

Features:
- **Auto-generated pairs**: Automatically creates cards for all system pairs
- **Side-by-side editors**: Each pair shows two code editors (one per system)
- **Load defaults**: Button to populate with system-specific default functions
- **Real-time validation**: Validates code as you type (syntax, structure, type hints)
- **Visual feedback**: Shows validation errors inline with line numbers
- **Smart saving**: Only saves when changes are made

### Navigation
Added to sidebar between "Configs" and "Schedules":
- Menu item: "Type Mappings"
- Route: `view === 'type-mappings'`

### API Service
New service in `frontend/src/services/api.js`:
```javascript
export const typeTransformationService = {
  list: () => ...
  get: (systemAId, systemBId) => ...
  create: (data) => ...
  update: (systemAId, systemBId, data) => ...
  delete: (systemAId, systemBId) => ...
  getDefault: (systemKind) => ...
  validateCode: (code) => ...
};
```

## Usage Flow

1. **Navigate to Type Mappings** view from sidebar
2. **View all system pairs** - automatically generated from active systems
3. **For each pair**:
   - Click "Load Default" to populate with system-specific defaults
   - Or write custom transformation logic
   - Code is validated in real-time
4. **Save** when ready - validation runs before saving
5. **Use in validations** - `run_validation.py` will automatically use these transformations

## Example Transformation Functions

### Databricks
```python
def transform_columns(column_name: str, data_type: str) -> str:
    if data_type == 'DoubleType()':
        return column_name
    if data_type.startswith('DecimalType'):
        return column_name
    if data_type == 'StringType()':
        return column_name
    return f"CAST({column_name} AS STRING)"
```

### Netezza
```python
def transform_columns(column_name: str, data_type: str) -> str:
    if data_type == 'DOUBLE PRECISION':
        return column_name
    if 'CHAR' in data_type:
        return f"RTRIM({column_name})"
    if data_type.startswith('NUMERIC'):
        return column_name
    return f"CAST({column_name} AS VARCHAR(250))"
```

## Files Modified/Created

### Backend
- ✅ `backend/sql/ddl.sql` - Added type_transformations table
- ✅ `backend/models.py` - Added TypeTransformationIn, TypeTransformationUpdate, ValidatePythonCode models
- ✅ `backend/app.py` - Added 7 new endpoints for type transformations
- ✅ `backend/default_transformations.py` - New file with default functions

### Frontend
- ✅ `frontend/src/views/TypeMappingsView.jsx` - New view component
- ✅ `frontend/src/views/index.js` - Exported TypeMappingsView
- ✅ `frontend/src/App.jsx` - Added TypeMappingsView to routing
- ✅ `frontend/src/components/Sidebar.jsx` - Added "Type Mappings" menu item
- ✅ `frontend/src/services/api.js` - Added typeTransformationService

## Next Steps (Not Implemented Yet)

1. **Update `run_validation.py`** to use these transformations:
   - Fetch transformation functions from database
   - Execute them dynamically using `exec()`
   - Apply to column transformations before comparison

2. **Database Migration** (if needed):
   - Run DDL to create the new table in your database
   - Consider adding default transformations for existing system pairs

3. **Testing**:
   - Test with real Netezza ↔ Databricks validation
   - Verify type transformations work correctly
   - Test edge cases (syntax errors, invalid functions)

## Security Considerations

- Code validation helps prevent basic errors
- `exec()` runs user-provided Python code - ensure this runs in a controlled environment
- Consider sandboxing execution in production
- Validation happens at save time to catch issues early

