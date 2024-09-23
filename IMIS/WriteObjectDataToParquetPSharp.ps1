param(
    [Parameter(Position=0, Mandatory=$true, HelpMessage="The name of SQL Server instance")]
    [string] $ServerName, 

    [Parameter(Position=1, Mandatory=$true, HelpMessage="Database name")]
    [string] $Database, 

    [Parameter(Position=1, Mandatory=$true, HelpMessage="Database login name")]
    [string] $dbUser, 

    [Parameter(Position=1, Mandatory=$true, HelpMessage="Database login password")]
    [string] $dbPwd, 

    [Parameter(Position=2, Mandatory=$true, HelpMessage="Query to select data")]
    [string] $Query, 

    [Parameter(Position=3, Mandatory=$true, HelpMessage="Schema name of object being exported")]
    [string] $SchemaName,

    [Parameter(Position=4, Mandatory=$true, HelpMessage="Table name of object being exported")]
    [string] $TableName, 

    [Parameter(Position=5, Mandatory=$true, HelpMessage="The path to a folder where a file will be created")]
    [string] $FilePath, 

    [Parameter(Position=8, Mandatory=$false, HelpMessage="Command timeout")]
    [int] $CommandTimeout = 30,

    [Parameter(Position=9, Mandatory=$false, HelpMessage="Maximum number of rows per rowgroup")]
    [int] $RowsPerRowGroup = 10000,
    
    [Parameter(Position=10, Mandatory=$false, HelpMessage="The script will report progress every XXX records")]
    [int] $ReportProgressFrequency = 10000,

    [Parameter(Position=11, Mandatory=$false, HelpMessage="Debug mode")]
    [bool] $DebugInfo = $false

)

function CreateDataArray {
    [CmdletBinding()] 
    param( 
        [Parameter(Position=0, Mandatory=$true)] $SchemaTable
    )

    $data = @{}
    foreach ($column in $SchemaTable.Rows) {
        $columnData = [System.Collections.ArrayList]::new()
        $data.Add($column.ColumnName, $columnData)
    }  
    return $data  
}

function WriteRowGroup {
    param( 
        [Parameter(Position=0, Mandatory=$true)] $FileWriter,
        [Parameter(Position=1, Mandatory=$true)] $Columns,
        [Parameter(Position=2, Mandatory=$true)] $DataTypes,
        [Parameter(Position=3, Mandatory=$true)] $Data
    )

    try {
        $rowGroup = $fileWriter.AppendRowGroup()

        foreach ($column in $Columns) {

            $dataArray = $data[$column.Name].ToArray($dataTypes[$column.Name])
            $columnWriter = $rowGroup.NextColumn().LogicalWriter()
            $columnWriter.WriteBatch($dataArray)
        } 
        $rowGroup.Close()  
    } finally {
        $rowGroup.Dispose()
    }
}


function Convert-SecureStringToString
{
  param
  (
    [Parameter(Mandatory,ValueFromPipeline)]
    [System.Security.SecureString]
    $Password
  )
  
  process
  {
    $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
    [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
  }
}


Function Get-Int96Pieces 
{
    [CmdletBinding()]
    param
    (
        [Parameter(ParameterSetName='DateTime', Position=0)]
        [DateTime] $dt,
        [Parameter(ParameterSetName='TimeSpan', Position=0)]
        [TimeSpan] $ts
    )

    if ($dt) {
        [long]$nanos = $dt.TimeOfDay.Ticks * 100 # 1000000
    } else {
        [long]$nanos = $ts.Ticks * 100
    }
    [int]$a = [BitConverter]::ToInt32([BitConverter]::GetBytes($nanos -band 0xFFFFFFFF))
    [int]$b = $nanos -shr 32
    
    if ($dt) {
        $Year = $dt.Year; $Month = $dt.Month; $Day = $dt.Day;                    
        if ($Month -lt 3)
        {
            $Month = $Month + 12;
            $Year = $Year - 1;
        }
        $c = [math]::Floor($Day + (153 * $Month - 457) / 5 + 365 * $Year + ($Year / 4) - ($Year / 100) + ($Year / 400) + 1721119)
    } else {
        $c = 2440589    # 1/2/1970 constant
    }
    
    return $a, $b, $c
}

enum SqlDataTypes {
    int
    tinyint
    smallint
    bigint
    bit
    char
    nchar
    varchar
    nvarchar
    real
    float
    decimal
    money
    smallmoney
    date
    time
    datetime
    datetime2
    smalldatetime
    datetimeoffset
    binary
    varbinary
    string
    uniqueidentifier
    ntext
    image
    numeric
    text
    sysname
    geography
}


Function Export-Table
{
    [CmdletBinding()] 
    param( 
        [Parameter(Position=0, Mandatory=$true)] [string]$ServerName,    
        [Parameter(Position=0, Mandatory=$true)] [string]$Database,
        [Parameter(Position=2, Mandatory=$true)] [string]$Query,
        [Parameter(Position=3, Mandatory=$true)] [string]$FilePath
    ) 

    $conn = New-Object System.Data.SqlClient.SqlConnection 
    #$password = ConvertFrom-SecureString -SecureString $global:Password -AsPlainText
    #$password = Convert-SecureStringToString $Password
    #$conn.ConnectionString = "Server={0};Database={1};User ID={2};Password={3};Trusted_Connection=False;Connect Timeout={4}" -f $ServerName,$Database,$Username,$Password,$global:ConnectionTimeout
    $conn.ConnectionString = "Server={0};Database={1};User ID={2};Password={3};Trusted_Connection=False;Connect Timeout={4}" -f $ServerName,$Database,$dbUser,$dbPwd,$global:ConnectionTimeout
    
    [string[]]$columnNames = @()
    [type[]]$columnTypes = @()
    [int[]]$sqlDataTypes = @()
    [string[]]$columnTypeNames = @()
    $dataTypes = @{}

    $columnsArray = [System.Collections.ArrayList]::new()
    
    try {
        $conn.Open() 
        $cmd = New-Object System.Data.SqlClient.SqlCommand
        $cmd.Connection = $conn
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $global:CommandTimeout
    
        $reader = $cmd.ExecuteReader()
        if($reader.FieldCount -and $reader.HasRows)
        {
    
            ####### SCHEMA definition ###############    
            $schemaTable = $reader.GetSchemaTable()

            foreach ($schemaColumn in $schemaTable.Rows) {
                $columnName = $schemaColumn.ColumnName
                $columnNames += $columnName
                $columnType = $schemaColumn.DataType
                $columnTypes += $columnType
                $columnTypeName = $schemaColumn.DataTypeName
                $columnTypeNames += $columnTypeName
                $isNull = $schemaColumn.AllowDBNull
        
                switch ($columnTypeName)
                {
                    {$_ -in @("int") } { 
                        $dataType = if ($isNull) { [Nullable[int]] } else { [int] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[int]]]::new($columnName) } else {[ParquetSharp.Column[int]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::int
                        continue
                    }
                    {$_ -in @("tinyint") } { 
                        $dataType = if ($isNull) { [Nullable[byte]] } else { [byte] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[byte]]]::new($columnName) } else {[ParquetSharp.Column[byte]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::tinyint
                        continue
                    }
                    {$_ -in @("smallint") } { 
                        $dataType = if ($isNull) { [Nullable[Int16]] } else { [Int16] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[Int16]]]::new($columnName) } else {[ParquetSharp.Column[Int16]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::smallint
                        continue
                    }
                    {$_ -in @("bigint") } { 
                        $dataType = if ($isNull) { [Nullable[long]] } else { [long] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[long]]]::new($columnName) } else {[ParquetSharp.Column[long]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::bigint
                        continue
                    }
                    {$_ -in @("bit") } { 
                        $dataType = if ($isNull) { [Nullable[bool]] } else { [bool] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[bool]]]::new($columnName) } else {[ParquetSharp.Column[bool]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::bit
                        continue
                    }
                    {$_ -in @("nvarchar", "varchar", "nchar", "char", "ntext", "text", "sysname", "xml") } { 
                        $dataType = [string]
                        $column = [ParquetSharp.Column[string]]::new($columnName)
                        $sqlDataTypes += [SqlDataTypes]::string
                        continue
                    }
                    
                    {$_ -in @("uniqueidentifier") } { 
                        $dataType =[string]
                        $column = [ParquetSharp.Column[string]]::new($columnName)
                        $sqlDataTypes += [SqlDataTypes]::uniqueidentifier
                        continue
                    }
                    {$_ -like "*geography" } { 
                        $dataType =[string]
                        $column = [ParquetSharp.Column[string]]::new($columnName)
                        $sqlDataTypes += [SqlDataTypes]::geography
                        continue
                    }
                    
                    {$_ -in @("date") } { 
                        $dataType = if ($isNull) { [Nullable[ParquetSharp.Date]] } else { [ParquetSharp.Date] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[ParquetSharp.Date]]]::new($columnName) } else {[ParquetSharp.Column[ParquetSharp.Date]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::date
                        continue
                    }
                    {$_ -in @("datetime") } { 
                        $dataType = if ($isNull) { [Nullable[ParquetSharp.Int96]] } else { [ParquetSharp.Int96] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[ParquetSharp.Int96]]]::new($columnName) } else {[ParquetSharp.Column[ParquetSharp.Int96]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::datetime
                        continue
                    }
                    {$_ -in @("datetime2") } { 
                        $dataType = if ($isNull) { [Nullable[ParquetSharp.Int96]] } else { [ParquetSharp.Int96] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[ParquetSharp.Int96]]]::new($columnName) } else {[ParquetSharp.Column[ParquetSharp.Int96]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::datetime2
                        continue
                    }
                    {$_ -in @("smalldatetime") } { 
                        $dataType = if ($isNull) { [Nullable[ParquetSharp.Int96]] } else { [ParquetSharp.Int96] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[ParquetSharp.Int96]]]::new($columnName) } else {[ParquetSharp.Column[ParquetSharp.Int96]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::smalldatetime
                        continue
                    }
                    {$_ -in @("time") } { 
                        $dataType = if ($isNull) { [Nullable[ParquetSharp.Int96]] } else { [ParquetSharp.Int96] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[ParquetSharp.Int96]]]::new($columnName) } else {[ParquetSharp.Column[ParquetSharp.Int96]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::time
                        continue
                    }
                    {$_ -in @("datetimeoffset") } { 
                        $dataType = if ($isNull) { [string] } else { [string] }
                        $column = [ParquetSharp.Column[string]]::new($columnName)
                        $sqlDataTypes += [SqlDataTypes]::datetimeoffset
                        continue
                    }
                    {$_ -in @("money", "smallmoney") } { 
                        $dataType = if ($isNull) { [Nullable[decimal]] } else { [decimal] }
                        $column = [ParquetSharp.Column[Nullable[decimal]]]::new($columnName, [ParquetSharp.LogicalType]::Decimal(29,4))
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[decimal]]]::new($columnName, [ParquetSharp.LogicalType]::Decimal(29,4)) } else {[ParquetSharp.Column[decimal]]::new($columnName, [ParquetSharp.LogicalType]::Decimal(29,4))}
                        $sqlDataTypes += [SqlDataTypes]::decimal
                        continue
                    }
                    {$_ -in @("real") } { 
                        $dataType = if ($isNull) { [Nullable[float]] } else { [float] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[float]]]::new($columnName) } else {[ParquetSharp.Column[float]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::real
                        continue
                    }
                    {$_ -in @("float") } { 
                        $dataType = if ($isNull) { [Nullable[double]] } else { [double] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[double]]]::new($columnName) } else {[ParquetSharp.Column[double]]::new($columnName)}
                        $sqlDataTypes += [SqlDataTypes]::float
                        continue
                    }
                    {$_ -in @("decimal", "numeric") } { 
                        $dataType = if ($isNull) { [Nullable[decimal]] } else { [decimal] }
                        $column = if ($isNull) { [ParquetSharp.Column[Nullable[decimal]]]::new($columnName, [ParquetSharp.LogicalType]::Decimal(29,$schemaColumn.NumericScale)) } else {[ParquetSharp.Column[decimal]]::new($columnName, [ParquetSharp.LogicalType]::Decimal(29,$schemaColumn.NumericScale))}
                        $sqlDataTypes += [SqlDataTypes]::decimal
                        continue
                    }
                    {$_ -in @("binary", "varbinary", "image", "timestamp") } { 
                        $dataType = [byte[]]
                        $column = [ParquetSharp.Column[byte[]]]::new($columnName)
                        $sqlDataTypes += [SqlDataTypes]::binary
                        continue
                    }
                    Default {
                        Write-Output "columnName : $columnName | columnTypeName : $columnTypeName"
                        Write-Log -errorLevel ERROR -message "Not Implemented; ColumnName : $columnName | ColumnTypeName : $columnTypeName"
                        throw "Not Implemented; ColumnName : $columnName | ColumnTypeName : $columnTypeName" }
                }
        
                $columnsArray.Add($column) | Out-Null
                $dataTypes.Add($columnName, $dataType)
            }
        
            $columns = $columnsArray.ToArray([ParquetSharp.Column])

            ####### End SCHEMA definition ###############

            
            
            
                $fileWriter = [ParquetSharp.ParquetFileWriter]::new($filePath, $columns)
                $data = CreateDataArray $schemaTable
                $rowNum = 0
                while ($reader.Read()) {
                    $rowNum++
                    for ([int]$i=0; $i -lt $columnNames.Length; $i++) {

                        $val = $null
                        $columnType = $columnTypes[$i]
                        $columnTypeName = $columnTypeNames[$i]
                        $sqlDataType = $sqlDataTypes[$i]

                        try{
                            if ($sqlDataType -eq [SqlDataTypes]::int)                { if (!$reader.IsDBNull($i)) { $val = $reader.GetInt32($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::decimal -or $sqlDataType -eq [SqlDataTypes]::numeric)        { if (!$reader.IsDBNull($i)) { $val = $reader.GetDecimal($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::date)           { if (!$reader.IsDBNull($i)) { $val = [ParquetSharp.Date]::new($reader.GetDateTime($i)) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::geography)      { if (!$reader.IsDBNull($i)) { $val =  $reader[$i].ToString() } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::string)         { if (!$reader.IsDBNull($i)) { $val = $reader.GetString($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::uniqueidentifier)         { if (!$reader.IsDBNull($i)) { $val = [string]$reader.GetSqlGuid($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::smallint)       { if (!$reader.IsDBNull($i)) { $val = $reader.GetInt16($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::tinyint)        { if (!$reader.IsDBNull($i)) { $val = $reader.GetByte($i) }  }
                            elseif ($sqlDataType -eq [SqlDataTypes]::bigint)         { if (!$reader.IsDBNull($i)) { $val = $reader.GetInt64($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::money)          { if (!$reader.IsDBNull($i)) { $val = $reader.GetDecimal($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::smallmoney)     { if (!$reader.IsDBNull($i)) { $val = $reader.GetDecimal($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::bit)            { if (!$reader.IsDBNull($i)) { $val = $reader.GetBoolean($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::real)           { if (!$reader.IsDBNull($i)) { $val = $reader.GetFloat($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::float)          { if (!$reader.IsDBNull($i)) { $val = $reader.GetDouble($i) } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::binary)         { if (!$reader.IsDBNull($i)) { $val = [byte[]]$reader[$i] } }
                            elseif ($sqlDataType -eq [SqlDataTypes]::time)           { if (!$reader.IsDBNull($i)) { 
                                        [TimeSpan]$ts = $reader.GetTimeSpan($i) 
                                        $a, $b, $c = Get-Int96Pieces $ts
                                        $val = [ParquetSharp.Int96]::new($a,$b,$c) } 
                                    }
                            elseif (($sqlDataType -eq [SqlDataTypes]::datetime) -or  ($sqlDataType -eq [SqlDataTypes]::smalldatetime) -or  ($sqlDataType -eq [SqlDataTypes]::datetime2))      { if (!$reader.IsDBNull($i)) { 
                                        [DateTime]$dt = $reader.GetDateTime($i) 
                                        $a, $b, $c = Get-Int96Pieces $dt
                                        $val = [ParquetSharp.Int96]::new($a,$b,$c) } 
                                    }
                            elseif ($sqlDataType -eq [SqlDataTypes]::datetimeoffset) { if (!$reader.IsDBNull($i)) { 
                                        [DateTimeOffset]$dto = $reader.GetDateTimeOffset($i) 
                                        $val = $dto.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", [cultureinfo]::InvariantCulture) }
                                    }
                            else { 
                                Write-Host "Database: $Database | Query: $Query | columnTypeName: $columnTypeName | columnName: $($columnNames[$i]) " -ForegroundColor Red
                                throw "Not Implemented; ColumnTypeName: $columnTypeName | ColumnName: $($columnNames[$i])" 
                                }
                            }
                            catch {
                                Write-Host "Database: $Database | Query: $Query | columnTypeName: $columnTypeName | columnName: $($columnNames[$i]) " -ForegroundColor Red
                                Write-Host $_.Exception.Message -ForegroundColor Red
                                Write-Host $_.Exception.InnerException -ForegroundColor Red
                                Write-Host $_.Exception.StackTrace -ForegroundColor Red
                                
                                Write-Log -errorLevel ERROR -message "Database: $Database | Query: $Query | columnTypeName: $columnTypeName | columnName: $($columnNames[$i]) "
                                Write-Log -errorLevel ERROR -message $_.Exception.Message -stack $_.Exception.StackTrace
                                throw
                            }
                        [void]$data[$columnNames[$i]].Add($val)             
                    }    

                    # Report progress
                    if ($rowNum % $ReportProgressFrequency -eq 0) {
                        Write-Output "$(Get-Date -Format hh:mm:ss.fff) - Job '$Database/ $SchemaName.$TableName' processed rows - $rowNum"
                        if ($DebugInfo){ Write-log -errorLevel INFO -message "Job '$Database/ $SchemaName.$TableName' processed rows - $rowNum" }
                    }

                    # Need to dump RowGroup
                    if ($rowNum % $RowsPerRowGroup -eq 0) {
                        WriteRowGroup $fileWriter $columns $dataTypes $data
                        $data = CreateDataArray $schemaTable
                    }
                }
            
                # more records available for the last RowGroup
                if ($rowNum%$RowsPerRowGroup -gt 0) {
                    WriteRowGroup $fileWriter $columns $dataTypes $data
                    Write-Output "$(Get-Date -Format hh:mm:ss.fff) - Job '$Database/ $SchemaName.$TableName'  processed rows - $rowNum"
                    Write-log -errorLevel INFO -message "Job '$Database/ $SchemaName.$TableName' processed rows - $rowNum"
                }

            


            
        }

        $reader.Close()

        
    }
    catch {
        Write-Host "Database: $Database | Query: $Query" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        Write-Host $_.Exception.InnerException -ForegroundColor Red

        Write-Log -errorLevel ERROR -message "Database: $Database | Query: $Query"
        Write-Log -errorLevel ERROR -message $_.Exception
        Write-Log -errorLevel ERROR -message $_.InvocationInfo
        throw
    } 
    finally {
        if ($conn) { $conn.Close() }
        if ($fileWriter) { $fileWriter.Dispose() }
    }    
}


Add-Type -Path "$PSScriptRoot\Setup\DLL\ParquetSharp.dll"
$startTime = Get-Date

if ($DebugInfo)
{ 
    Write-Output "$(Get-Date -Format hh:mm:ss.fff) - Job '$Database/ $SchemaName.$TableName'  calling function Export-Table" 
    Write-log -errorLevel INFO -message "Job '$Database/ $SchemaName.$TableName'  calling function Export-Table"
}
Export-Table -ServerName $ServerName -Database $database -Query $query -FilePath $filePath
if ($DebugInfo)
{
    Write-Output "$(Get-Date -Format hh:mm:ss.fff) - Job '$Database/ $SchemaName.$TableName'  completed"
    Write-log -errorLevel INFO -message "Job '$Database/ $SchemaName.$TableName'  completed"
}


$finishTime = Get-Date

if ($DebugInfo) {
    Write-Host "Program Start Time:   ", $startTime -ForegroundColor Green
    Write-Host "Program Finish Time:  ", $finishTime -ForegroundColor Green
    Write-Host "Program Elapsed Time: ", ($finishTime-$startTime) -ForegroundColor Green
    Write-log -errorLevel INFO -message "Program Elapsed Time:   $startTime"
}