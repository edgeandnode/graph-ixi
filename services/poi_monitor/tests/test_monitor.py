import pytest
from unittest.mock import Mock, patch
from src.monitor import main
from src.database import Database
from src.notification import SlackNotifier
from src.analyzer import PoiAnalyzer

def test_main_initializes_components():
    """Test that main properly initializes all components."""
    with patch('src.monitor.Database') as mock_db_class, \
         patch('src.monitor.SlackNotifier') as mock_notifier_class, \
         patch('src.monitor.PoiAnalyzer') as mock_analyzer_class, \
         patch('src.monitor.time.sleep', side_effect=KeyboardInterrupt):  # Break the loop
        
        # Setup mocks
        mock_db = Mock(spec=Database)
        mock_notifier = Mock(spec=SlackNotifier)
        mock_analyzer = Mock(spec=PoiAnalyzer)
        
        mock_db_class.return_value = mock_db
        mock_notifier_class.return_value = mock_notifier
        mock_analyzer_class.return_value = mock_analyzer
        
        # Run main (will be interrupted by KeyboardInterrupt)
        main()
        
        # Verify components were initialized
        mock_db_class.assert_called_once()
        mock_notifier_class.assert_called_once()
        mock_analyzer_class.assert_called_once_with(mock_db, mock_notifier)

def test_main_handles_initialization_error():
    """Test that main properly handles initialization errors."""
    with patch('src.monitor.Database', side_effect=Exception("Test error")), \
         patch('src.monitor.logger') as mock_logger:
        
        with pytest.raises(Exception):
            main()
        
        mock_logger.error.assert_called_once()

def test_main_processes_submissions():
    """Test that main calls process_new_submissions."""
    with patch('src.monitor.Database') as mock_db_class, \
         patch('src.monitor.SlackNotifier') as mock_notifier_class, \
         patch('src.monitor.PoiAnalyzer') as mock_analyzer_class, \
         patch('src.monitor.time.sleep', side_effect=KeyboardInterrupt):  # Stop after first run
        
        # Setup mocks
        mock_analyzer = Mock(spec=PoiAnalyzer)
        mock_analyzer_class.return_value = mock_analyzer
        
        # Run main
        main()
        
        # Verify process_new_submissions was called
        assert mock_analyzer.process_new_submissions.call_count == 1

def test_main_handles_keyboard_interrupt():
    """Test that main handles keyboard interrupt gracefully."""
    with patch('src.monitor.Database') as mock_db_class, \
         patch('src.monitor.SlackNotifier') as mock_notifier_class, \
         patch('src.monitor.PoiAnalyzer') as mock_analyzer_class, \
         patch('src.monitor.time.sleep', side_effect=KeyboardInterrupt), \
         patch('src.monitor.logger') as mock_logger:
        
        main()
        
        mock_logger.info.assert_any_call("Shutting down POI Monitor service...") 