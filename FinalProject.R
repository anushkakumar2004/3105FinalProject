library(dplyr)
library(ggplot2)
library(tidyr)
library(readr)
library(lubridate)
library(caret)
library(car)
library(tseries)
library(forecast)

###### READ IN DATA ###### 
## Dependent Variable
cash <- read_csv("cash.csv")

## Independent Variables
apr25 <- read_csv("apr25.csv")
feb25 <- read_csv("feb25.csv")
dec24 <- read_csv("dec24.csv")
#beef <- read_csv("beef.csv")
consumersentiment <- read_csv("consumersentiment.csv")
corn <- read_csv("corn.csv")
crudeoil <- read_csv("crudeoil.csv")
dollar <- read_csv("dollar.csv")
hogs <- read_csv("hogs.csv")
inflation <- read_csv("inflation.csv")
interestrate <- read_csv("interestrate.csv")
naturalgas <- read_csv("naturalgas.csv")
placements <- read_csv("placements.csv")
soybeans <- read_csv("soybean.csv")
sp500 <- read_csv("sp500.csv")
wheat <- read_csv("wheat.csv")


##### PROCESS THE DATA #####
## Clean up Consumer Sentiment Data
colnames(consumersentiment) <- consumersentiment[1, ] # First Row becomes Column Name
consumersentiment <- consumersentiment[-1, ] # Remove First Row
consumersentiment <- consumersentiment %>%
  separate(col = 'Month,Year,Index,', into = c("Month", "Year", "Index"), sep = ",", convert = TRUE) # Separate into three columns
consumersentiment <- consumersentiment %>% # Convert columns to appropriate data types
  mutate(
    Month = as.integer(Month),
    Year = as.integer(Year),
    Index = as.numeric(Index)
  )

## Clean up Placements Data
placements_processed <- placements %>%
  select(Year, Period, Value) %>%  # Select relevant columns
  mutate(Value = as.numeric(gsub(",", "", Value))) %>%
  group_by(Year, Period) %>%
  summarize(Total_Placements = sum(Value, na.rm = TRUE), .groups = "drop") %>% # Group by Year & Month and Sum
  mutate(Date = as.Date(paste(Year, Period, "01", sep = "-"), format = "%Y-%b-%d")) # Date Column
placements_daily <- placements_processed %>% # Expand to a daily dataframe
  rowwise() %>%
  mutate(Date = list(seq.Date(from = Date, to = ceiling_date(Date, "month") - 1, by = "day"))) %>%
  unnest(Date) %>%
  select(Date, Total_Placements) # Duplicate Total Placements Per Day

## Clean Up Weather Data
weather <- read_csv("3868676.csv")
weather <- weather %>%
  drop_na() %>% # Drop Empty Rows
  select(-STATION) %>%
  pivot_wider(
    names_from = NAME, # Reformat Per Station/City  
    values_from = TAVG  
  ) %>%
  rename ( # Rename the Columns
    Nebraska_Temp = 'GRAND ISLAND CENTRAL NE REGIONAL AIRPORT, NE US',
    Texas_Temp = 'AMARILLO AIRPORT, TX US',
    Kansas_Temp = 'DODGE CITY REGIONAL AIRPORT, KS US',
    Date = 'DATE'
  )

## Combine All Data
consumersentiment <- consumersentiment %>% # Convert the Consumer Sentiment from Monthly to Daily Data
  mutate(Date = as.Date(paste(Year, Month, "01", sep = "-"))) %>%
  select(Date, Index)
consumersentiment_expanded <- consumersentiment %>%
  rowwise() %>%
  mutate(
    Date = list(seq.Date(from = Date, to = ceiling_date(Date, "month") - 1, by = "day")) 
  ) %>%
  unnest(Date)
# Prepare Each Individual Dataset for the Join Function
cash <- cash %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
corn <- corn %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
dollar <- dollar %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
hogs <- hogs %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
crudeoil <- crudeoil %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
dec24 <- dec24 %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
apr25 <- apr25 %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
feb25 <- feb25 %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
interestrate <- interestrate %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
inflation <- inflation %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
naturalgas <- naturalgas %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
soybeans <- soybeans %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
sp500 <- sp500 %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
wheat <- wheat %>%
  select(Date = Time, Open = Open) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"))
# Merge All Datesets by Date
combined_data <- consumersentiment_expanded %>% 
  left_join(cash, by = "Date", suffix = c("", "_cash")) %>%
  left_join(corn, by = "Date", suffix = c("", "_corn")) %>%
  left_join(dollar, by = "Date", suffix = c("", "_dollar")) %>%
  left_join(hogs, by = "Date", suffix = c("", "_hogs")) %>%
  left_join(crudeoil, by = "Date", suffix = c("", "_oil")) %>%
  left_join(feb25, by = "Date", suffix = c("", "_feb25")) %>%
  left_join(inflation, by = "Date", suffix = c("", "_inflation")) %>%
  left_join(interestrate, by = "Date", suffix = c("", "_interestrate")) %>%
  left_join(naturalgas, by = "Date", suffix = c("", "_naturalgas")) %>%
  left_join(soybeans, by = "Date", suffix = c("", "_soybeans")) %>%
  left_join(sp500, by = "Date", suffix = c("", "_sp500")) %>%
  left_join(wheat, by = "Date", suffix = c("", "_wheat")) %>%
  left_join(dec24, by = "Date", suffix = c("", "_dec24")) %>%
  left_join(placements_daily, by = "Date") %>%
  left_join(weather, by = "Date") %>%
  drop_na() %>%
  arrange(Date)
# Add a "Month" variable (1 for January, 2 for February, etc.)
combined_data$Month <- as.numeric(format(combined_data$Date, "%m"))


##### SUMMARY STATISTICS #####
# Trend of Cattle Cash Price in One Year
plot(combined_data$Date, combined_data$Open,
  type = "o",                     # Lines/Points
  col = "seagreen3",              
  pch = 16,                       # Filled Circles
  lwd = 2,                        
  xlab = "Date",                 
  ylab = "Open Price",            
  main = "Cattle Cash Prices Over Time",
  cex.axis = 1.2,                 
  cex.lab = 1.4,                  
  cex.main = 1.5)     
# Relationship Between Futures Contracts and Underlying Cash Price
ggplot(combined_data, aes(x = Open_dec24, y = Open)) +
  geom_point() +
  geom_smooth(method = "lm", col = "blue4") +
  labs(title = "Cattle Cash Prices vs. Dec24 Contract",
       x = "Dec24 Contract",
       y = "Cash Prices") +
  theme_minimal()


##### ANALYSIS #####
## Stationarity Test 
adf_test <- adf.test(combined_data$Open, alternative = "stationary")
print(adf_test)
# Take the first difference
combined_data$Open_diff <- c(NA, diff(combined_data$Open))
# Check stationarity again
adf_test_diff <- adf.test(na.omit(combined_data$Open_diff), alternative = "stationary")
print(adf_test_diff)

## Baseline Model 1: Linear Equation
set.seed(123) 
# 80% of data for training; 20% for testing
train_indices <- createDataPartition(combined_data$Open, p = 0.8, list = FALSE)
train_data <- combined_data[train_indices, ]
test_data <- combined_data[-train_indices, ]
# Linear Model
lm_model <- lm(Open ~ . - Date - Open_diff - Open_feb25 - Kansas_Temp - Open_interestrate - Open_soybeans - Open_inflation - Nebraska_Temp - Month - Total_Placements - Open_dollar - Open_sp500 - Texas_Temp, data = train_data)
# Check for Multicollinearity
vif(lm_model)
# Display the summary of the model
summary(lm_model)
# Make Predictions on the Test Data
test_data$baseline_predicted <- predict(lm_model, newdata = test_data)
plot(test_data$Open, test_data$baseline_predicted,
  xlab = "Actual Cash Prices",
  ylab = "Predicted Cash Prices",
  main = "Predicted vs Actual Cash Prices",
  pch = 16,           
  col = "blue")
abline(0, 1, col = "red", lwd = 2, lty = 2)  # Perfect Prediction
# Evaluate the Model
baseline_mae <- mean(abs(test_data$Open - test_data$baseline_predicted), na.rm = TRUE)
baseline_rmse <- sqrt(mean((test_data$Open - test_data$baseline_predicted)^2, na.rm = TRUE))
# Print Results of Validation
cat("Baseline MAE:", baseline_mae, "\n")
cat("Baseline RMSE:", baseline_rmse, "\n")
  
## Baseline Model 2: ARIMA 
arima_model <- auto.arima(train_data$Open_diff)
# Summary of Model
summary(arima_model)
# Assume the Last Value of Open is Known
last_open <- tail(train_data$Open, 1)
# Forecast Open_diff on ARIMA Model
forecast_values <- forecast(arima_model, h = nrow(test_data))$mean
# Reintegrate to get Predicted Open Values
forecast_open <- cumsum(c(last_open, forecast_values))  
# Get Residuals
test_residuals <- test_data$Open - forecast_open 
# Validate Model
test_mae <- mean(abs(test_residuals))
test_rmse <- sqrt(mean(test_residuals^2))
# Print Results of Validation
cat("Test MAE:", test_mae, "\n")
cat("Test RMSE:", test_rmse, "\n")
# Check Residuals
checkresiduals(arima_model)
# Create New DataFrame to Compare Forecasts to Actual Cash Prices
results <- data.frame(
  Date = test_data$Date,             
  Actual_Open = test_data$Open,     
  Forecasted_Open = forecast_open[-1]   
)
# Plot Forecasted vs Actual
plot(results$Date, results$Actual_Open, type = "l", col = "blue", lwd = 2,
     xlab = "Date", ylab = "Open Price", main = "Actual vs Forecasted Prices")
lines(results$Date, results$Forecasted_Open, col = "red", lwd = 2)
legend("topright", legend = c("Actual", "Forecasted"), col = c("blue", "red"), lty = 1, lwd = 2)

## Model: ARIMAX
# Prepare X Regressors
xreg_train <- as.matrix(train_data[, c("Index", "Open_corn", 
                                       "Open_hogs", "Open_oil", "Open_naturalgas",
                                       "Open_wheat", "Open_dec24")])
xreg_test <- as.matrix(test_data[, c("Index", "Open_corn", 
                                     "Open_hogs", "Open_oil", "Open_naturalgas",
                                     "Open_wheat", "Open_dec24")])
arimax_model <- auto.arima(train_data$Open_diff, xreg = xreg_train)
# Summary of Model
summary(arimax_model)
# Check Residuals
checkresiduals(arimax_model)
last_open <- tail(train_data$Open, 1)
# Forecast Open_diff
forecast_values_x <- forecast(arimax_model, xreg = xreg_test)$mean
# Reintegrate to get Open Values
forecast_open_x <- cumsum(c(last_open, forecast_values_x)) 
# Get Residuals
test_residuals_x <- test_data$Open - forecast_open_x 
# Validate Model
test_mae_x <- mean(abs(test_residuals_x))
test_rmse_x <- sqrt(mean(test_residuals_x^2))
# Print Validation Results
cat("Test MAE:", test_mae_x, "\n")
cat("Test RMSE:", test_rmse_x, "\n")
# Check Residuals
checkresiduals(arimax_model)
# Create New DataFrame to Compare Forecasts to Actual Cash Prices
results_x <- data.frame(
  Date = test_data$Date,            
  Actual_Open = test_data$Open,      
  Forecasted_Open = forecast_open_x[-1]    )
# Plot Forecasted vs Actual
plot(results_x$Date, results_x$Forecasted_Open, 
  type = "l", col = "blue", lwd = 2,
  xlab = "Date", ylab = "Open Price", 
  main = "Actual vs Forecasted Prices",
  ylim = range(c(results_x$Actual_Open, results_x$Forecasted_Open)) 
) 
lines(
  results_x$Date, results_x$Actual_Open, 
  col = "red", lwd = 2
) 
legend(
  "topright", 
  legend = c("Forecasted", "Actual"), 
  col = c("blue", "red"), 
  lty = 1, lwd = 2)