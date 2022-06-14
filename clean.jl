using CSV
using DataFrames

"""
remove_rows_symbols removes all rows with the special characters
in the set : {*, ", \$, ~}
"""
function remove_rows_symbols!(df)
	expr = r"[~,*,$,\"]"
	filter!(x -> !occursin(expr, x.Value), df)
end

"""
remove_symbols removes the same special characters as above
from the column with column_name.
"""
function remove_symbols!(df, column_name)

	for r in eachrow(df)
		str = r[column_name]
		expr = r"[~,*,$,\"]"
		matches = collect(eachmatch(expr, str))
		if !isempty(matches)
			ranges_to_remove = [m.offset:(m.offset + sizeof(m.match)) for m in matches]
			ranges_to_keep = setdiff(1:sizeof(str), reduce(vcat, ranges_to_remove))
			r.Value = String(codeunits(str)[ranges_to_keep])
		end
	end

	return df
end

""""
remove_numbers_from_ID removes the suffix of ID that looks like -5-1
so that accurate comparison to the language strings can happen
since there are multiple languages that share the same substring
such as ENGLISH, OLD_ENGLISH, etc.
"""
remove_numbers_from_ID(ID) = ID[1:(findfirst(isequal('-'), ID)-1)]

"""
filter_languages! keeps the rows of df whose ID after suffix removal
matches one of the language IDs in the langs vector
"""
function filter_languages!(df, langs)
	filter!(df) do r
		ID = remove_numbers_from_ID(r.ID)
		any(ID .== langs)
	end
end

df = CSV.File("./data/forms.csv") |> DataFrame
remove_symbols!(df, "Value")
remove_symbols!(df, "Form")

langs = ["DANISH", 
		"STANDARD_GERMAN", 
		"DUTCH", 
		"ENGLISH", 
		"FRENCH", 
		"ITALIAN", 
		"PORTUGUESE", 
		"SPANISH", 
		"GREEK"]

filter_languages!(df, langs)

CSV.write("./data/clean.csv", df)

