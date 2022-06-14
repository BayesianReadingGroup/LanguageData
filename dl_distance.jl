
# old distance function I wrote - Conor

# see https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance

# original ref
#  Indexing methods for approximate dictionary searching: Comparative analysis
#  L Boytsov - Journal of Experimental Algorithmics


mutable struct Metric
    aL::Int64
    bL::Int64
    m::Array{Int64}
    function Metric(aL,bL)
        m=zeros(Int64,(aL+2,bL+2))

        maxdist = aL+bL
        m[1, 1] = maxdist

        for i = 0:aL
            m[i+2,1]=maxdist
            m[i+2,2]=i
        end
        
        for j = 0:bL
            m[1,j+2]=maxdist
            m[2,j+2]=j
        end
        
        new(aL,bL,m)
    end
end

function get(d::Metric,i::Int64,j::Int64)
    d.m[i+2,j+2]
end

function set!(d::Metric,i::Int64,j::Int64,x::Int64)
    d.m[i+2,j+2]=x
end



function dLDistance(a::String,b::String)

#    input: strings a[1..length(a)], b[1..length(b)]
#    output: distance, integer

    da=Dict{Char,Int64}()
    
    aL=length(a)
    bL=length(b)

    d=Metric(aL,bL)
        
    for i = 1:aL

        db = 0
        
        for j = 1:bL

            k = get!(da,b[j],0)

            l = db
            
            if a[i] == b[j] 
                cost = 0
                db = j
            else
                cost = 1
            end
            
            newDIJ = minimum([get(d,i-1, j-1) + cost,  #substitution
                            get(d,i,j-1) + 1,        #insertion
                            get(d,i-1,j) + 1,        #deletion
                            get(d,k-1,l-1) + (i-k-1) + 1 + (j-l-1) #transposition
                            ])

            set!(d,i,j,newDIJ)
            
            da[a[i]] = i

        end
    end
    
    get(d,aL,bL)

end
