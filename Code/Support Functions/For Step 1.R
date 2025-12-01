## ----------------------------------------------------------------
## Define the coding parameters used in the environment.
##
##      Authors: Shelby Golden, MS from Yale's YSPH DSDE group
## Date Created: May 15th, 2025
## 
## Description: All custom functions used in the raw data cleaning
##              and preparation process. Much of this content was written
##              with the assistance of Yale's AI Clarity.
##
## Functions
##    1. preprocess_address: This function standardizes the format of an 
##       address string to facilitate checking for address similarity. It 
##       performs the following steps:
##          1. Converts all characters to lowercase.
##          2. Normalizes spaces around commas and retains commas.
##          3. Removes all non-alphanumeric characters except for commas and spaces.
##          4. Normalizes multiple spaces to a single space.
##          5. Trims leading and trailing whitespace.
## 
##    2. find_components: This function performs Depth-First Search (DFS) to 
##       find all nodes in the connected component. It's used to identify similar 
##       addresses within a specified tolerance range, creating unique groups. 
##       Utilized in the `find_similar_addresses()` function.
## 
##    3. find_similar_addresses: This function groups addresses based on their 
##       similarity using a specified threshold. It preprocesses the addresses, 
##       builds a similarity graph, and identifies groups of similar addresses.
## 
##    4. find_first_one: Finds the date column name where the first 1 
##       occurs. Used for arranging the rows associated with one ABI 
##       in descending order: i.e. older address to recent address.
## 

## ----------------------------------------------------------------
## FUNCTIONS

preprocess_address <- function(address) {
  #' @description 
  #' This function standardizes the format of an address string to facilitate
  #' checking for address similarity. It performs the following steps:
  #' 1. Converts all characters to lowercase.
  #' 2. Normalizes spaces around commas and retains commas.
  #' 3. Removes all non-alphanumeric characters except for commas and spaces.
  #' 4. Normalizes multiple spaces to a single space.
  #' 5. Trims leading and trailing whitespace.
  #'
  #' @param address A string containing the address to be standardized.
  #'
  #' @return A cleaned and standardized address string.
  
  # Convert to lowercase
  address <- tolower(address)
  
  # Normalize spaces around commas and retain commas in addresses
  address <- gsub("\\s*,\\s*", ", ", address)
  
  # Remove all characters except alphanumeric characters, commas, and spaces
  address <- gsub("[^a-z0-9, ]", "", address)
  
  # Normalize multiple spaces to a single space
  address <- gsub("\\s+", " ", address)
  
  # Trim leading and trailing whitespace
  address <- gsub("^\\s+|\\s+$", "", address)
  
  return(address)
}



find_components <- function(node, visited, address_graph) {
  #' @description
  #' This function performs Depth-First Search (DFS) to find all nodes in 
  #' the connected component. It's used to identify similar addresses within
  #' a specified tolerance range, creating unique groups. Utilized in
  #' the `find_similar_addresses()` function.
  #' 
  #'
  #' @param node An integer representing the starting node in the undirected graph. 
  #'             Each node represents similar addresses defined by the 
  #'             `stringdist(method = "jw")` function.
  #'             
  #' @param visited A logical vector indicating whether a node has been visited.
  #' 
  #' @param address_graph A list where each element contains the indices of
  #'                      its neighboring nodes.
  #'
  #' @return A vector containing all nodes in the connected component of the graph.
  
  
  # Initialize stack with the starting node and create an empty vector to store 
  # the connected component nodes.
  stack <- c(node)
  component <- c()
  
  # Perform DFS until the stack is empty.
  while (length(stack) > 0) {
    # After getting the top node in the stack, remove it.
    top <- stack[length(stack)]
    stack <- stack[-length(stack)]
    
    if (!visited[top]) {
      # Mark the node as visited.
      visited[top] <- TRUE
      # Add the node to the connected component.
      component <- c(component, top)
      # Add the neighbors of the node to the stack.
      stack <- c(stack, address_graph[[top]])
    }
  }
  return(component)
}



find_similar_addresses <- function(addresses, threshold = 0.15) {
  #' @description
  #' This function groups addresses based on their similarity using a specified 
  #' threshold. It preprocesses the addresses, builds a similarity graph, and 
  #' identifies groups of similar addresses.
  #' 
  #' @param addresses A character vector containing the addresses to be grouped.
  #' @param threshold A numeric value specifying the similarity threshold 
  #'                  (default is 0.15). Addresses with a similarity score 
  #'                  below this threshold are considered similar.
  #' 
  #' @return A list where each element is a group of similar addresses. Only
  #'         gives the uniquely defined address, and does not list redundancies.
  
  
  # Preprocess addresses to standardize the format.
  processed_addresses <- sapply(addresses, preprocess_address)
  n <- length(processed_addresses)
  
  # Initialize graph.
  address_graph <- vector("list", n)
  
  # Build the similarity graph.
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      # Compute the similarity between address nodes i and address j by adding
      # an edge from i to j and j to i, respectively.
      if (stringdist(processed_addresses[i], processed_addresses[j], method = "jw") < threshold) {
        address_graph[[i]] <- c(address_graph[[i]], j)
        address_graph[[j]] <- c(address_graph[[j]], i)
      }
    }
  }
  
  # Initialize the visited vector and a list to store unique groups.
  visited <- rep(FALSE, n)
  unique_groups <- list()
  
  # Find connected components for each unvisited node.
  for (i in 1:n) {
    if (!visited[i]) {
      component <- find_components(i, visited, address_graph)
      unique_groups <- c(unique_groups, list(sort(unique(addresses[component]))))
    }
  }
  
  # Convert address groups to strings and filter out duplicates.
  string_groups <- sapply(unique_groups, function(group) paste(sort(group), collapse = " ||| "))
  unique_string_groups <- unique(string_groups)
  unique_address_groups <- lapply(unique_string_groups, function(sgroup) unlist(strsplit(sgroup, " \\|\\|\\| ")))
  
  return(unique_address_groups)
}



find_first_one <- function(...) {
  #' @description
  #' This function finds the first column where a 1 occurs in a given row of a 
  #' data frame. It is used for arranging rows in descending order, from older 
  #' dates to more recent dates.
  #' 
  #' @param ... Variable arguments representing the elements of a row in a given 
  #'            data frame.
  #' 
  #' @return A character string representing the name of the first column where 
  #'         a 1 occurs. If no 1 is found, returns NA.
  
  
  # Convert the row elements into a single vector.
  row <- c(...)
  
  # Find the index of the first occurrence of 1.
  first_one_index <- which(row == 1)
  
  if (length(first_one_index) == 0) {
    # If there is no 1 in the row, return NA.
    return(NA)
    
  } else {
    # Return the name of the first column where a 1 occurs, removing any "X" 
    # prefix added to numeric column names.
    return(str_replace(names(row)[first_one_index[1]], "X", ""))
    
  }
}






