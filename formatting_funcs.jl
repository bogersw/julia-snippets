module FormattingFuncs

export format_integer, format_float, format_number, format_numbers

using DataFrames
using Printf

"""
Format an integer number as a string with a `thousands separator` that can be specified
(default `.`).
"""
function format_integer(number::Integer;
                        thousandsSeparator::String=".")::String
    # Format number as string for easier processing
    negative_number::Bool = number < 0
    number_string::String = @sprintf("%.0f", negative_number ? -1 * number : number)
    # start = start position of processing, stop = end position of processing
    start::Int64 = 1 + length(number_string) % 3
    stop::Int64 = length(number_string) - length(number_string) % 3
    # Get the various parts of the number that need to be separated
    results::Vector{String} = Vector{String}()
    if number_string[1:start-1] != ""
        push!(results, number_string[1:start-1])
    end
    results = [results ; [number_string[i:i+2] for i in start:3:stop]]
    # Return the formatted integer as a string
    if negative_number
        return "-" * join(results, thousandsSeparator)
    end
    return join(results, thousandsSeparator)
end

"""
Format a floating point number with a `decimal separator` and a `thousands separator`,
both of which can be specified. Defaults: decimal `,`, thousands `.`.
"""
function format_float(number::Float64;
                      thousandsSeparator::String=".",
                      decimalSeparator::String=",",
                      decimalPlaces::Int64=2)::String
    # Get the fractional and integral parts of the specified number as strings
    fpart, ipart = modf(round(number, digits=decimalPlaces))
    negative_number::Bool = number < 0
    ipart_string::String = format_integer(Int(ipart), thousandsSeparator=thousandsSeparator)
    fpart_string::String = @sprintf("%.50f", negative_number ? -1 * fpart : fpart)
    # Determine length of fractional part
    fpart_length = minimum([2 + decimalPlaces, length(fpart_string)])
    # Return formatted float number as a string
    if decimalPlaces == 0
        return ipart_string
    end
    return ipart_string * decimalSeparator * fpart_string[3:fpart_length]
end

"""
Format an integer or a floating point number as a string with a `decimal separator` and a
`thousands separator`, both of which can be specified. Defaults: decimal `,`, thousands `.`.

For floating point numbers the number of `decimal places` can be specified (default `2`).

For integer numbers there is an option to `show decimals` (default `false`).
"""
function format_number(number::T;
                       thousandsSeparator::String=".",
                       decimalSeparator::String=",",
                       decimalPlaces::Int64=2,
                       intShowDecimals::Bool=false)::String where T<:Real
    # Determine number type and call appropriate formatting function
    if isa(number, Integer)
        if intShowDecimals
            return format_float(Float64(number), thousandsSeparator=thousandsSeparator,
                                decimalSeparator=decimalSeparator, decimalPlaces=decimalPlaces)
        else
            return format_integer(Int(number), thousandsSeparator=thousandsSeparator)
        end
    elseif isa(number, Float16) || isa(number, Float32) || isa(number, Float64)
        return format_float(Float64(number), thousandsSeparator=thousandsSeparator,
                            decimalSeparator=decimalSeparator, decimalPlaces=decimalPlaces)
    else
        return string(number)
    end
end

"""
Format a vector of numbers (integer and / or floating point) as strings with a `decimal separator`
and a `thousands separator`, both of which can be specified. Defaults: decimal `,`, thousands `.`.

When a `DataFrame` is passed, all number columns will be formatted as strings: the same separators
as mentioned for vectors of numbers apply.

For floating point numbers the number of `decimal places` can be specified (default `2`).

For integer numbers there is an option to `show decimals` (default `false`).
"""
function format_numbers(v::Vector{T};
                        thousandsSeparator::String=".",
                        decimalSeparator::String=",",
                        decimalPlaces::Int64=2,
                        intShowDecimals::Bool=false)::Vector{String} where T<:Real
    return format_number.(v, thousandsSeparator=thousandsSeparator, decimalSeparator=decimalSeparator,
                          decimalPlaces=decimalPlaces, intShowDecimals=intShowDecimals)
end

function format_numbers(df::DataFrame;
                        thousandsSeparator::String=".",
                        decimalSeparator::String=",",
                        decimalPlaces::Int64=2,
                        intShowDecimals::Bool=false)::DataFrame
    dfc = copy(df)
    # Format number colums (type -> String)
    number_cols = propertynames(select(dfc, findall(col -> all(value -> value isa Union{Missing, Number}, col), eachcol(dfc))))
    foreach(col -> dfc[!, col] = format_numbers(dfc[!, col], thousandsSeparator=thousandsSeparator, decimalSeparator=decimalSeparator,
                                                decimalPlaces=decimalPlaces, intShowDecimals=intShowDecimals), number_cols)
    return dfc
end

end