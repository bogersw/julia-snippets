module DataFrameFuncs

export transpose_dataframe, first_rows, last_rows, find_rows, column_types,
       count_missing, replace_missing!, remove_cols_missing!, describe_number_cols,
       describe_string_cols, describe_date_cols

using DataFrames
using CSV
using Dates

"""
Transpose rows / columns of DataFrame `df`, starting at row `startrow` and using
`nrows` rows. Return value is a DataFrame.
"""
function transpose_dataframe(df::DataFrame,
                             startrow::Integer = 1,
                             nrows::Integer = 1)::DataFrame
    # Check input
    nrows_df::Int64 = size(df, 1)
    if startrow < 1 || nrows < 1
        throw(error("ERROR: invalid start row or number of rows specified, both should be >= 1."))
    elseif startrow > nrows_df
        throw(error("ERROR: start row exceeds number of rows in DataFrame."))
    end
    # Transpose a specific number of rows
    # Check if requested number of rows is available: otherwise limit number of rows
    dft::DataFrame = DataFrame(Column=names(df))
    for row in startrow:minimum([startrow + nrows - 1, nrows_df])
        dft[!, Symbol("Row", row)] = collect(df[row, :])
    end
    return dft
end

"""
Get first `n` rows of DataFrame `df`. Optionally `transpose` rows / columns or limit
the `columns` which are shown in the results. Return value is a DataFrame.
"""
function first_rows(df::DataFrame,
                    n::Integer;
                    transpose::Bool=false,
                    columns::Vector=[])::DataFrame
    dff::DataFrame = first(df, n)
    if !isempty(columns)
        dff = dff[:, columns]
    end
    transpose ? (return transpose_dataframe(dff, 1, n)) : return dff
end

"""
Get last `n` rows of DataFrame `df`. Optionally `transpose` rows / columns or limit
the `columns` which are shown in the results. Return value is a DataFrame.
"""
function last_rows(df::DataFrame,
                   n::Integer;
                   transpose::Bool=false,
                   columns::Vector{Symbol}=[])::DataFrame
    dfl::DataFrame = last(df, n)
    if !isempty(columns)
        dfl = dfl[:, columns]
    end
    transpose ? (return transpose_dataframe(dfl, 1, n)) : return dfl
end

"""
Find rows in DataFrame `df` for which the column `keycolumn` contains the value
`searchvalue`. Optionally limit the `columns` which are shown in the results.
Return value is a DataFrame.
"""
function find_rows(df::DataFrame,
                   keycolumn::Union{String, Symbol},
                   searchvalue::Any; transpose::Bool=false,
                   columns::Vector{Symbol}=[])::DataFrame
    if isempty(columns)
        dfr = df[df[:, keycolumn] .== searchvalue, :]
    else
        dfr = df[df[:, keycolumn] .== searchvalue, columns]
    end
    transpose ? (return transpose_dataframe(dfr, 1, nrow(dfr))) : return dfr
end

"""
Return the columns and their `type` of the columns in DataFrame `df`.
Columns are represented as symbols. Return value should be a sorted Dict(*).

(*) Note that sort of a Dict returns an OrderedCollections.OrderedDict{Symbol, Type}
    instead of a Dict{Symbol, Type} as one would expect. This is a known issue:

    https://github.com/JuliaCollections/OrderedCollections.jl/issues/25
"""
function column_types(df::DataFrame)
    return sort(Dict(Symbol.(names(df)) .=> eltype.(eachcol(df))))
end

"""
Return a `DataFrame` which contains the columns with `missing`
values of DataFrame `df` (and the number of missing values).
"""
function count_missing(df::DataFrame)::DataFrame
    return subset(describe(df, :nmissing), :nmissing => x -> x .> 0)
end

"""
Replace missing values in a `column` of DataFrame `df` with another
`value` (number / string). Changes the original DataFrame.
"""
function replace_missing!(df::DataFrame, column::Symbol, value::Union{Real, String})
    replace!(df[!, column], missing => value);
end

"""
Identify and remove columns with only `missing` values. Changes the original
DataFrame.
"""
function remove_cols_missing!(df::DataFrame)
    dfm::DataFrame = describe(df, :nmissing)
    cols_missing = dfm[dfm[!, :nmissing] .== nrow(df), :][:, 1]
    select!(df, Not(cols_missing))
end

"""
Describe various properties of the number columns in the DataFrame `df`. The output
columns are presented as strings with formatting for better readability.
"""
function describe_number_cols(df::DataFrame)::DataFrame
    # Select number columns from describe DataFrame
    dfn::DataFrame = describe(df, :eltype, :nmissing, :mean, :q25, :median, :q75, :min, :max, sum => :sum)
    number_cols = propertynames(select(df, findall(col -> all(value -> value isa Union{Missing, Number}, col), eachcol(df))))
    dfn = dfn[in(number_cols).(dfn.variable), :] |> sort
    # Change types of number columns (type -> Real)
    foreach(col -> dfn[!, col] = convert(Vector{Real}, dfn[!, col]),
            [:nmissing, :mean, :q25, :median, :q75, :min, :max, :sum])
    return dfn
end

"""
Describe various properties of the string columns in the DataFrame `df`. One of these properties
is a list of unique items, of which the number of items shown is controlled by `max_unique_items`
(default `6`).
"""
function describe_string_cols(df::DataFrame;
                              max_unique_items::Int64=6)::DataFrame
    # Select string columns from describe DataFrame
    dfs = describe(df, :eltype, :nmissing, :nunique)
    string_cols = propertynames(select(df, findall(col -> all(value -> value isa Union{Missing, AbstractString}, col), eachcol(df))))
    dfs = dfs[in(string_cols).(dfs.variable), :]
    # Change types of number columns (type -> Real)
    foreach(col -> dfs[!, col] = convert(Vector{Real}, dfs[!, col]),
            [:nmissing, :nunique])
    # Determine unique items
    vec_unique::Vector{String} = []
    for (idx, col) in enumerate(dfs[:, :variable])
        if dfs[idx, :nunique] <= max_unique_items
            push!(vec_unique, join( unique(df[:, col]) |> sort, ", ") )
        else
            push!(vec_unique, ">$max_unique_items")
        end
    end
    return hcat(dfs, Dict(:unique_items => vec_unique) |> DataFrame)
end

"""
Describe various properties of the date columns in the DataFrame `df`. Dates are formatted
as strings according to the specified `dateFormat` (default `dd-mm-yyyy`).
"""
function describe_date_cols(df::DataFrame;
                            dateFormat::String="dd-mm-yyyy")::DataFrame
    # Select relevant columns from describe DataFrame
    dfd = describe(df, :eltype, :nmissing, :nunique, :min, :max)
    date_cols = propertynames(select(df, findall(col -> all(value -> value isa Union{Missing, Dates.Date}, col), eachcol(df))))
    dfd = dfd[in(date_cols).(dfd.variable), :]
    # Change types of number columns (type -> Real)
    foreach(col -> dfd[!, col] = convert(Vector{Real}, dfd[!, col]),
            [:nmissing, :nunique])
    # Change types of date columns (type -> String)
    foreach(col -> dfd[!, col] = Dates.format.(dfd[!, col], dateFormat),
            [:min, :max])
    return dfd
end

end